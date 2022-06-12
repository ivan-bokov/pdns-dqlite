package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/canonical/go-dqlite/app"
	"github.com/canonical/go-dqlite/client"
	"github.com/ivan-bokov/pdns-dqlite/backend"
	"github.com/ivan-bokov/pdns-dqlite/backend/core"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

func main() {
	var host string
	var cluster *[]string
	var dir string
	var dnssec bool
	var api string
	cmd := &cobra.Command{
		Use:   "pdns-dqlite",
		Short: "Имплементация backend Power DNS на базе dqlite",
		Long:  "Имплементация backend Power DNS на базе dqlite",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := os.Mkdir(dir, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
				return errors.Wrapf(err, "не могу создать %s", dir)
			}
			logFunc := func(l client.LogLevel, msg string, a ...interface{}) {
				log.Printf(fmt.Sprintf("%s: \n", msg), a...)
			}
			opts := make([]app.Option, 0)
			opts = append(opts, app.WithAddress(host))
			if cluster != nil {
				opts = append(opts, app.WithCluster(*cluster))
			}
			opts = append(opts, app.WithLogFunc(logFunc))
			dqlite, err := app.New(dir, opts...)
			if err != nil {
				return errors.Wrap(err, "Ошибка создания экземпляра dqlite")
			}
			if err = dqlite.Ready(context.Background()); err != nil {
				return errors.Wrap(err, "Экземпляр dqlite не готов к работе")
			}
			db, err := dqlite.Open(context.Background(), "power-dns")
			if err != nil {
				return errors.Wrap(err, "Ошибка открытия базы данных к работе")
			}
			if _, err = db.Exec(schema()); err != nil {
				log.Fatal(err)
			}

			svc := core.New(db, dnssec)
			handler := backend.New(svc)
			go func() {
				if err = handler.InitRoutes().Run(api); err != nil {
					log.Fatal(err)
				}
			}()
			ch, cancel := signal.NotifyContext(context.Background(), syscall.SIGPWR, syscall.SIGINT, syscall.SIGQUIT)
			defer cancel()
			<-ch.Done()

			return nil
		},
	}
	flags := cmd.Flags()
	flags.StringVarP(&api, "api", "a", "", "address used to expose the API")
	flags.StringVarP(&host, "host", "", "", "address used for internal database replication")
	cluster = flags.StringSliceP("cluster", "c", nil, "database addresses of existing nodes")
	flags.StringVarP(&dir, "dir", "D", "/tmp/power-dns", "data directory")
	flags.BoolVarP(&dnssec, "dnssec", "", false, "")

	err := cmd.MarkFlagRequired("api")
	if err != nil {
		log.Fatal(err)
	}
	err = cmd.MarkFlagRequired("host")
	if err != nil {
		log.Fatal(err)
	}

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func schema() string {
	return `
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE IF NOT EXISTS domains (
  id                    INTEGER PRIMARY KEY,
  name                  VARCHAR(255) NOT NULL COLLATE NOCASE,
  master                VARCHAR(128) DEFAULT NULL,
  last_check            INTEGER DEFAULT NULL,
  type                  VARCHAR(6) NOT NULL,
  notified_serial       INTEGER DEFAULT NULL,
  account               VARCHAR(40) DEFAULT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS name_index ON domains(name);
CREATE TABLE IF NOT EXISTS records (
  id                    INTEGER PRIMARY KEY,
  domain_id             INTEGER DEFAULT NULL,
  name                  VARCHAR(255) DEFAULT NULL,
  type                  VARCHAR(10) DEFAULT NULL,
  content               VARCHAR(65535) DEFAULT NULL,
  ttl                   INTEGER DEFAULT NULL,
  prio                  INTEGER DEFAULT NULL,
  disabled              BOOLEAN DEFAULT 0,
  ordername             VARCHAR(255),
  auth                  BOOL DEFAULT 1,
  FOREIGN KEY(domain_id) REFERENCES domains(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS records_lookup_idx ON records(name, type);
CREATE INDEX IF NOT EXISTS records_lookup_id_idx ON records(domain_id, name, type);
CREATE INDEX IF NOT EXISTS records_order_idx ON records(domain_id, ordername);
CREATE TABLE IF NOT EXISTS supermasters (
  ip                    VARCHAR(64) NOT NULL,
  nameserver            VARCHAR(255) NOT NULL COLLATE NOCASE,
  account               VARCHAR(40) NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ip_nameserver_pk ON supermasters(ip, nameserver);
CREATE TABLE IF NOT EXISTS comments (
  id                    INTEGER PRIMARY KEY,
  domain_id             INTEGER NOT NULL,
  name                  VARCHAR(255) NOT NULL,
  type                  VARCHAR(10) NOT NULL,
  modified_at           INT NOT NULL,
  account               VARCHAR(40) DEFAULT NULL,
  comment               VARCHAR(65535) NOT NULL,
  FOREIGN KEY(domain_id) REFERENCES domains(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS comments_idx ON comments(domain_id, name, type);
CREATE INDEX IF NOT EXISTS comments_order_idx ON comments (domain_id, modified_at);
CREATE TABLE IF NOT EXISTS domainmetadata (
 id                     INTEGER PRIMARY KEY,
 domain_id              INT NOT NULL,
 kind                   VARCHAR(32) COLLATE NOCASE,
 content                TEXT,
 FOREIGN KEY(domain_id) REFERENCES domains(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS  domainmetaidindex ON domainmetadata(domain_id);
CREATE TABLE IF NOT EXISTS cryptokeys (
 id                     INTEGER PRIMARY KEY,
 domain_id              INT NOT NULL,
 flags                  INT NOT NULL,
 active                 BOOL,
 published              BOOL DEFAULT 1,
 content                TEXT,
 FOREIGN KEY(domain_id) REFERENCES domains(id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE INDEX IF NOT EXISTS domainidindex ON cryptokeys(domain_id);
CREATE TABLE IF NOT EXISTS tsigkeys (
 id                     INTEGER PRIMARY KEY,
 name                   VARCHAR(255) COLLATE NOCASE,
 algorithm              VARCHAR(50) COLLATE NOCASE,
 secret                 VARCHAR(255)
);
CREATE UNIQUE INDEX IF NOT EXISTS namealgoindex ON tsigkeys(name, algorithm);
COMMIT;`
}
