package core

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ivan-bokov/pdns-dqlite/backend/core/db"
	"github.com/pkg/errors"
)

type Service struct {
	dnssec bool
	db     *sql.DB
	mu     sync.Mutex
	tx     map[int]*sql.Tx
}

func New(db *sql.DB, dnssec bool) *Service {
	return &Service{
		dnssec: dnssec,
		db:     db,
		tx:     map[int]*sql.Tx{},
	}
}

func (s *Service) SetNotified(domainID int, serial int) error {
	stmt, args, err := db.Prepare(
		"update-serial-query",
		"serial", serial,
		"domain_id", domainID,
	)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(stmt, args...)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) setLastCheck(domainID int, lastcheck int64) error {
	stmt, args, err := db.Prepare(
		"update-lastcheck-query",
		"last_check", lastcheck,
		"domain_id", domainID,
	)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(stmt, args...)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) SetFresh(domainID int) error {
	return s.setLastCheck(domainID, time.Now().UTC().Unix())
}

func (s *Service) Lookup(qtype string, qname string, zoneID int) ([]*DNSResourceRecord, error) {
	var err error
	var stmt string
	var args []interface{}
	listRR := make([]*DNSResourceRecord, 0)
	if qtype != "ANY" {
		if zoneID < 0 {
			stmt, args, err = db.Prepare(
				"basic-query",
				"qtype", qtype,
				"qname", qname,
			)
		} else {
			stmt, args, err = db.Prepare(
				"id-query",
				"qtype", qtype,
				"qname", qname,
				"domain_id", zoneID,
			)
		}
	} else {
		if zoneID < 0 {
			stmt, args, err = db.Prepare(
				"any-query",
				"qname", qname,
			)
		} else {
			stmt, args, err = db.Prepare(
				"any-id-query",
				"qname", qname,
				"domain_id", zoneID,
			)
		}
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(stmt, args...)
	for rows.Next() {
		rr := new(DNSResourceRecord)
		err = rows.Scan(&rr.Content, &rr.TTL, &rr.Prio, &rr.Qtype, &rr.DomainID, &rr.Disabled, &rr.Qname, &rr.Auth)
		if err != nil {
			//TODO Добавить логирование
			continue
		}
		listRR = append(listRR, rr)
	}
	return listRR, err
}

func (s *Service) List(zonename string, domainID int, includeDisabled bool) ([]*DNSResourceRecord, error) {
	listRR := make([]*DNSResourceRecord, 0)
	if domainID < 0 {
		stmt, args, err := db.Prepare(
			"get-domain-id",
			"domain", zonename,
		)
		if err != nil {
			return listRR, err
		}
		rows, err := s.db.Query(stmt, args...)
		if err != nil {
			return listRR, err
		}
		if rows.Next() {
			err = rows.Scan(domainID)
			if err != nil {
				return listRR, err
			}
		} else {
			return listRR, errors.New(fmt.Sprintf("Domain not found: %s", zonename))
		}
	}
	stmt, args, err := db.Prepare(
		"list-query",
		"include_disabled", includeDisabled,
		"domain_id", domainID,
	)
	if err != nil {
		return listRR, err
	}
	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return listRR, err
	}
	for rows.Next() {
		rr := new(DNSResourceRecord)
		err = rows.Scan(&rr.Content, &rr.TTL, &rr.Prio, &rr.Qtype, &rr.DomainID, &rr.Disabled, &rr.Qname, &rr.Auth)
		if err != nil {
			//TODO Добавить логирование
			continue
		}
		listRR = append(listRR, rr)
	}
	return listRR, err
}

func (s *Service) GetBeforeAndAfterNamesAbsolute(id int, qname string) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	return errors.New("No implementation")
}

func (s *Service) SetDomainMetadata(name string, kind string, meta []string) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	stmt, args, err := db.Prepare("clear-domain-metadata-query",
		"domain", name,
		"kind", kind,
	)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(stmt, args...)
	errs := make([]error, 0)
	if len(meta) != 0 {
		for _, m := range meta {
			stmt, args, err = db.Prepare("set-domain-metadata-query",
				"kind", kind,
				"content", m,
				"domain", name,
			)
			if err != nil {
				errs = append(errs, errors.New(fmt.Sprintf("%v", err)))
				continue
			}
			_, err = s.db.Exec(stmt, args...)
			if err != nil {
				errs = append(errs, errors.New(fmt.Sprintf("%v", err)))
			}
		}
	}
	if len(errs) != 0 {
		return errors.New(fmt.Sprintf("Unable to set metadata kind %s for domain %s", kind, name))
	}
	return nil
}

func (s *Service) AddDomainKey(name string, key *KeyData) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	stmt, args, err := db.Prepare("add-domain-key-query",
		"domain", name,
		"flags", key.Flags,
		"active", key.Active,
		"published", key.Published,
		"content", key.Content,
	)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(stmt, args...)
	return nil
}

func (s *Service) FeedRecord(rr *DNSResourceRecord, ordername string) error {
	var oName interface{}
	prio := 0
	auth := true
	content := rr.Content
	if rr.Qtype == "MX" || rr.Qtype == "SRV" {
		pos := FindFirstNotOf(content, "0123456789")
		if pos != -1 {
			//TODO Сделать очистку до первых цифр
		}
	}
	if s.dnssec {
		auth = rr.Auth
	}
	if ordername == "" {
		oName = nil
	} else {
		oName = []byte(strings.ToLower(ordername))
	}
	stmt, args, err := db.Prepare("insert-record-query",
		"content", content,
		"ttl", rr.TTL,
		"priority", prio,
		"qtype", rr.Qtype,
		"domain_id", rr.DomainID,
		"disabled", rr.Disabled,
		"qname", rr.Qname,
		"auth", auth,
		"ordername", oName,
	)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(stmt, args...)
	return err
}

func (s *Service) CreateSlaveDomain(ip string, domain string) error {
	stmt, args, err := db.Prepare("insert-zone-query",
		"domain", domain,
		"account", "",
		"masters", fmt.Sprintf("%s:53", ip),
		"type", "SLAVE",
	)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(stmt, args...)
	return err
}

func (s *Service) GetAllDomainMetadata(name string) (map[string][]string, error) {
	meta := make(map[string][]string)
	stmt, args, err := db.Prepare(
		"get-all-domain-metadata-query",
		"domain", name,
	)
	if err != nil {
		return meta, err
	}
	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return meta, err
	}
	for rows.Next() {
		var m1, m2 string
		err = rows.Scan(m1, m2)
		if err != nil {
			return make(map[string][]string), err
		}
		if _, ok := meta[m1]; !ok {
			meta[m1] = make([]string, 0, 10)
		}
		meta[m1] = append(meta[m1], m2)
	}
	return meta, nil
}

func (s *Service) GetDomainInfo(name string) (*DomainInfo, error) {
	stmt, args, err := db.Prepare(
		"info-zone-query",
		"domain", name,
	)
	if err != nil {
		return new(DomainInfo), err
	}

	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return new(DomainInfo), err
	}
	di := new(DomainInfo)
	if rows.Next() {
		master := ""
		err = rows.Scan(&di.ID, &di.Zone, &master, &di.LastCheck, &di.Serial, &di.Kind, &di.Account)
		if err != nil {
			log.Println("[ERROR] " + err.Error())
			return new(DomainInfo), err
		}
		if master != "" {
			di.Master = StringTok(master, " ,\t")
		}
	}
	return di, nil
}

func (s *Service) GetAllDomains(includeDisabled bool) ([]*DomainInfo, error) {
	stmt, args, err := db.Prepare(
		"get-all-domains-query",
		"include_disabled", includeDisabled,
	)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	dis := make([]*DomainInfo, 0, 10)
	for rows.Next() {
		di := new(DomainInfo)
		master := ""
		err = rows.Scan(&di.ID, &di.Zone, &master, &di.LastCheck, &di.Serial, &di.Kind, &di.Account)
		if err != nil {
			log.Println("[ERROR] " + err.Error())
			return nil, err
		}
		if master != "" {
			di.Master = StringTok(master, " ,\t")
		}
		dis = append(dis, di)
	}
	return dis, nil
}
func (s *Service) GetDomainMetadata(name string, kind string) ([]string, error) {
	stmt, args, err := db.Prepare(
		"get-domain-metadata-query",
		"domain", name,
		"kind", kind,
	)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	metas := make([]string, 0, 10)
	for rows.Next() {
		meta := ""
		err = rows.Scan(&meta)
		if err != nil {
			log.Println("[ERROR] " + err.Error())
			return nil, err
		}
		metas = append(metas, meta)
	}
	return metas, nil
}

func (s *Service) GetDomainKeys(name string) ([]*KeyData, error) {
	if !s.dnssec {
		return nil, errors.New("Only for DNSSEC")
	}
	stmt, args, err := db.Prepare(
		"list-domain-keys-query",
		"domain", name,
	)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	keys := make([]*KeyData, 0, 10)
	for rows.Next() {
		key := new(KeyData)
		var active string
		var published string
		err = rows.Scan(&key.ID, &key.Flags, &active, &published, &key.Content)
		if err != nil {
			return nil, err
		}
		key.Active = active == "1"
		key.Published = published == "1"
		keys = append(keys, key)
	}
	return keys, nil
}

func (s *Service) RemoveDomainKey(name string, id int) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	stmt, args, err := db.Prepare(
		"remove-domain-key-query",
		"domain", name,
		"key_id", id,
	)
	if err != nil {
		return err
	}

	if _, err = s.db.Exec(stmt, args...); err != nil {
		return err
	}
	return nil
}
func (s *Service) ActivateDomainKey(name string, id int) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	stmt, args, err := db.Prepare(
		"activate-domain-key-query",
		"domain", name,
		"key_id", id,
	)
	if err != nil {
		return err
	}

	if _, err = s.db.Exec(stmt, args...); err != nil {
		return err
	}
	return nil
}
func (s *Service) DeactivateDomainKey(name string, id int) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	stmt, args, err := db.Prepare(
		"deactivate-domain-key-query",
		"domain", name,
		"key_id", id,
	)
	if err != nil {
		return err
	}

	if _, err = s.db.Exec(stmt, args...); err != nil {
		return err
	}
	return nil
}
func (s *Service) PublishDomainKey(name string, id int) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	stmt, args, err := db.Prepare(
		"publish-domain-key-query",
		"domain", name,
		"key_id", id,
	)
	if err != nil {
		return err
	}

	if _, err = s.db.Exec(stmt, args...); err != nil {
		return err
	}
	return nil
}
func (s *Service) UnPublishDomainKey(name string, id int) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	stmt, args, err := db.Prepare(
		"unpublish-domain-key-query",
		"domain", name,
		"key_id", id,
	)
	if err != nil {
		return err
	}

	if _, err = s.db.Exec(stmt, args...); err != nil {
		return err
	}
	return nil
}
func (s *Service) GetTSIGKey(name string) (*string, *string, error) {
	stmt, args, err := db.Prepare(
		"get-tsig-key-query",
		"key_name", name,
	)
	if err != nil {
		return nil, nil, err
	}
	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return nil, nil, err
	}
	var algorithm *string
	var content *string
	for rows.Next() {
		row0, row1 := "", ""
		err = rows.Scan(&row0, &row1)
		if err != nil {
			return nil, nil, err
		}
		if algorithm == nil {
			algorithm = &row0
			algorithm = &row1
		}
	}
	return algorithm, content, nil
}

func (s *Service) SuperMasterBackend(ip string, domain string, nsset []*DNSResourceRecord) (*string, *string, error) {
	for _, rr := range nsset {
		stmt, args, err := db.Prepare(
			"supermaster-query",
			"ip", ip,
			"nameserver", rr.Content,
		)
		if err != nil {
			return nil, nil, err
		}
		rows, err := s.db.Query(stmt, args...)
		if err != nil {
			return nil, nil, err
		}
		for rows.Next() {
			account := ""
			err = rows.Scan(&account)
			if err != nil {
				return nil, nil, err
			}
			return &rr.Content, &account, nil
		}
	}
	return nil, nil, nil
}

func (s *Service) ReplaceRRSet(trxid, domain_id int, qname string, qt string, rrset []*DNSResourceRecord) error {
	var tx *sql.Tx
	var ok bool
	if tx, ok = s.tx[trxid]; !ok {
		return errors.New("replaceRRSet called outside of transaction")
	}
	if qt != "ANY" {
		stmt, args, err := db.Prepare(
			"delete-rrset-query",
			"domain_id", domain_id,
			"qname", qname,
			"qtype", qt,
		)
		if err != nil {
			return err
		}
		_, err = tx.Exec(stmt, args...)
		if err != nil {
			return err
		}
	} else {
		stmt, args, err := db.Prepare(
			"delete-names-query",
			"domain_id", domain_id,
			"qname", qname,
		)
		if err != nil {
			return err
		}
		_, err = tx.Exec(stmt, args...)
		if err != nil {
			return err
		}
	}
	if len(rrset) == 0 {
		stmt, args, err := db.Prepare(
			"delete-comment-rrset-query",
			"domain_id", domain_id,
			"qname", qname,
			"qtype", qt,
		)
		if err != nil {
			return err
		}
		_, err = tx.Exec(stmt, args...)
		if err != nil {
			return err
		}
	}
	for _, rr := range rrset {
		err := s.FeedRecord(rr, "")
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) FeedEnts(trxid, domain_id int, nonterm map[string]bool) error {
	var tx *sql.Tx
	var ok bool
	if tx, ok = s.tx[trxid]; !ok {
		return errors.New("replaceRRSet called outside of transaction")
	}
	for qname, auth := range nonterm {
		stmt, args, err := db.Prepare(
			"insert-empty-non-terminal-order-query",
			"domain_id", domain_id,
			"qname", qname,
			"ordername", nil,
			"auth", (auth || s.dnssec),
		)
		if err != nil {
			return err
		}
		_, err = tx.Exec(stmt, args...)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) FeedEnts3(trxid, domain_id int, domain string, nonterm map[string]bool, narrow bool) error {
	if !s.dnssec {
		return errors.New("Only for DNSSEC")
	}
	var tx *sql.Tx
	var ok bool
	if tx, ok = s.tx[trxid]; !ok {
		return errors.New("replaceRRSet called outside of transaction")
	}
	var ordername *string
	for qname, auth := range nonterm {
		if narrow || !auth {
			// TODO: Нужно реализовать хэш функцию
			// ordername =
		}
		stmt, args, err := db.Prepare(
			"insert-empty-non-terminal-order-query",
			"domain_id", domain_id,
			"qname", qname,
			"ordername", ordername,
			"auth", auth,
		)
		if err != nil {
			return err
		}
		_, err = tx.Exec(stmt, args...)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) StartTransaction(trxid, domain_id int) error {
	if _, ok := s.tx[trxid]; ok {
		return errors.New("Транзакция начата")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	s.tx[trxid] = tx
	if domain_id > 0 {
		stmt, args, err := db.Prepare(
			"delete-zone-query",
			"domain_id", domain_id,
		)
		if err != nil {
			return err
		}
		_, err = tx.Exec(stmt, args...)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) CommitTransaction(trxid int) error {
	if _, ok := s.tx[trxid]; !ok {
		return errors.New("Транзакция отсутствует")
	}
	tx := s.tx[trxid]
	return tx.Commit()
}
func (s *Service) AbortTransaction(trxid int) error {
	if _, ok := s.tx[trxid]; !ok {
		return errors.New("Транзакция отсутствует")
	}
	tx := s.tx[trxid]
	return tx.Rollback()
}

func (s *Service) SearchRecords(pattern string, maxResult int) ([]*DNSResourceRecord, error) {
	escapedPattern := Pattern2SQLPattern(pattern)
	stmt, args, err := db.Prepare(
		"search-records-query",
		"value", escapedPattern,
		"value2", escapedPattern,
		"limit", maxResult,
	)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	rrset := make([]*DNSResourceRecord, 0, 10)
	for rows.Next() {
		rr := new(DNSResourceRecord)
		err = rows.Scan(&rr.Content, &rr.TTL, &rr.Prio, &rr.Qtype, &rr.DomainID, &rr.Disabled, &rr.Qname, &rr.Auth)
		if err != nil {
			return nil, err
		}
		rrset = append(rrset, rr)
	}
	return rrset, nil
}

func (s *Service) GetUpdatedMasters() ([]*DomainInfo, error) {
	stmt, args, err := db.Prepare(
		"info-all-master-query",
	)
	if err != nil {
		return nil, err
	}
	rows, err := s.db.Query(stmt, args...)
	if err != nil {
		return nil, err
	}
	updatedDomains := make([]*DomainInfo, 0, 10)
	var parts []string
	for rows.Next() {
		di := new(DomainInfo)
		var content string
		var notifiedSerial int64
		err = rows.Scan(&di.ID, &di.Zone, &notifiedSerial, &content)
		if err != nil {
			return nil, err
		}
		parts = StringTok(content, "")
		serial := int64(0)
		if len(parts) > 2 {
			serial, err = strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if serial != notifiedSerial {
			di.Serial = serial
			di.NotifiedSerial = notifiedSerial
			updatedDomains = append(updatedDomains, di)
		}
	}
	return updatedDomains, nil
}
