package db

import (
	"github.com/ivan-bokov/pdns-dqlite/backend/core/sql"
	"github.com/pkg/errors"
)

var declare map[string]string

func init() {
	declare = make(map[string]string)
	record_query := "SELECT content,ttl,prio,type,domain_id,disabled,name,auth FROM records WHERE"

	declare["basic-query"] = record_query + " disabled=0 and type=:qtype and name=:qname"
	declare["id-query"] = record_query + " disabled=0 and type=:qtype and name=:qname and domain_id=:domain_id"
	declare["any-query"] = record_query + " disabled=0 and name=:qname"
	declare["any-id-query"] = record_query + " disabled=0 and name=:qname and domain_id=:domain_id"
	declare["list-query"] = "SELECT content,ttl,prio,type,domain_id,disabled,name,auth,ordername FROM records WHERE (disabled=0 OR :include_disabled) and domain_id=:domain_id order by name, type"
	declare["list-subzone-query"] = record_query + " disabled=0 and (name=:zone OR name like :wildzone) and domain_id=:domain_id"

	declare["remove-empty-non-terminals-from-zone-query"] = "delete from records where domain_id=:domain_id and type is null"
	declare["delete-empty-non-terminal-query"] = "delete from records where domain_id=:domain_id and name=:qname and type is null"

	declare["info-zone-query"] = "select id,name,master,last_check,notified_serial,type,account from domains where name=:domain"

	declare["get-domain-id"] = "select id from domains where name=:domain"

	declare["info-all-slaves-query"] = "select id,name,master,last_check from domains where type='SLAVE'"
	declare["supermaster-query"] = "select account from supermasters where ip=:ip and nameserver=:nameserver"
	declare["supermaster-name-to-ips"] = "select ip,account from supermasters where nameserver=:nameserver and account=:account"
	declare["supermaster-add"] = "insert into supermasters (ip, nameserver, account) values (:ip,:nameserver,:account)"
	declare["autoprimary-remove"] = "delete from supermasters where ip = :ip and nameserver = :nameserver"
	declare["list-autoprimaries"] = "select ip,nameserver,account from supermasters"

	declare["insert-zone-query"] = "insert into domains (type,name,master,account,last_check,notified_serial) values(:type, :domain, :masters, :account, null, null)"

	declare["insert-record-query"] = "insert into records (content,ttl,prio,type,domain_id,disabled,name,ordername,auth) values (:content,:ttl,:priority,:qtype,:domain_id,:disabled,:qname,:ordername,:auth)"
	declare["insert-empty-non-terminal-order-query"] = "insert into records (type,domain_id,disabled,name,ordername,auth,ttl,prio,content) values (null,:domain_id,0,:qname,:ordername,:auth,null,null,null)"

	declare["get-order-first-query"] = "select ordername from records where disabled=0 and domain_id=:domain_id and ordername is not null order by 1 asc limit 1"
	declare["get-order-before-query"] = "select ordername, name from records where disabled=0 and ordername <= :ordername and domain_id=:domain_id and ordername is not null order by 1 desc limit 1"
	declare["get-order-after-query"] = "select min(ordername) from records where disabled=0 and ordername > :ordername and domain_id=:domain_id and ordername is not null"
	declare["get-order-last-query"] = "select ordername, name from records where disabled=0 and ordername != '' and domain_id=:domain_id and ordername is not null order by 1 desc limit 1"

	declare["update-ordername-and-auth-query"] = "update records set ordername=:ordername,auth=:auth where domain_id=:domain_id and name=:qname and disabled=0"
	declare["update-ordername-and-auth-type-query"] = "update records set ordername=:ordername,auth=:auth where domain_id=:domain_id and name=:qname and type=:qtype and disabled=0"
	declare["nullify-ordername-and-update-auth-query"] = "update records set ordername=NULL,auth=:auth where domain_id=:domain_id and name=:qname and disabled=0"
	declare["nullify-ordername-and-update-auth-type-query"] = "update records set ordername=NULL,auth=:auth where domain_id=:domain_id and name=:qname and type=:qtype and disabled=0"

	declare["update-master-query"] = "update domains set master=:master where name=:domain"
	declare["update-kind-query"] = "update domains set type=:kind where name=:domain"
	declare["update-account-query"] = "update domains set account=:account where name=:domain"
	declare["update-serial-query"] = "update domains set notified_serial=:serial where id=:domain_id"
	declare["update-lastcheck-query"] = "update domains set last_check=:last_check where id=:domain_id"
	declare["info-all-master-query"] = "select domains.id, domains.name, domains.notified_serial, records.content from records join domains on records.domain_id=domains.id and records.name=domains.name where records.type='SOA' and records.disabled=0 and domains.type='MASTER'"
	declare["delete-domain-query"] = "delete from domains where name=:domain"
	declare["delete-zone-query"] = "delete from records where domain_id=:domain_id"
	declare["delete-rrset-query"] = "delete from records where domain_id=:domain_id and name=:qname and type=:qtype"
	declare["delete-names-query"] = "delete from records where domain_id=:domain_id and name=:qname"

	declare["add-domain-key-query"] = "insert into cryptokeys (domain_id, flags, active, published, content) select id, :flags, :active, :published, :content from domains where name=:domain"
	declare["get-last-inserted-key-id-query"] = "select last_insert_rowid()"
	declare["list-domain-keys-query"] = "select cryptokeys.id, flags, active, published, content from domains, cryptokeys where cryptokeys.domain_id=domains.id and name=:domain"
	declare["get-all-domain-metadata-query"] = "select kind,content from domains, domainmetadata where domainmetadata.domain_id=domains.id and name=:domain"
	declare["get-domain-metadata-query"] = "select content from domains, domainmetadata where domainmetadata.domain_id=domains.id and name=:domain and domainmetadata.kind=:kind"
	declare["clear-domain-metadata-query"] = "delete from domainmetadata where domain_id=(select id from domains where name=:domain) and domainmetadata.kind=:kind"
	declare["clear-domain-all-metadata-query"] = "delete from domainmetadata where domain_id=(select id from domains where name=:domain)"
	declare["set-domain-metadata-query"] = "insert into domainmetadata (domain_id, kind, content) select id, :kind, :content from domains where name=:domain"
	declare["activate-domain-key-query"] = "update cryptokeys set active=1 where domain_id=(select id from domains where name=:domain) and  cryptokeys.id=:key_id"
	declare["deactivate-domain-key-query"] = "update cryptokeys set active=0 where domain_id=(select id from domains where name=:domain) and  cryptokeys.id=:key_id"
	declare["publish-domain-key-query"] = "update cryptokeys set published=1 where domain_id=(select id from domains where name=:domain) and  cryptokeys.id=:key_id"
	declare["unpublish-domain-key-query"] = "update cryptokeys set published=0 where domain_id=(select id from domains where name=:domain) and  cryptokeys.id=:key_id"
	declare["remove-domain-key-query"] = "delete from cryptokeys where domain_id=(select id from domains where name=:domain) and cryptokeys.id=:key_id"
	declare["clear-domain-all-keys-query"] = "delete from cryptokeys where domain_id=(select id from domains where name=:domain)"
	declare["get-tsig-key-query"] = "select algorithm, secret from tsigkeys where name=:key_name"
	declare["set-tsig-key-query"] = "replace into tsigkeys (name,algorithm,secret) values(:key_name,:algorithm,:content)"
	declare["delete-tsig-key-query"] = "delete from tsigkeys where name=:key_name"
	declare["get-tsig-keys-query"] = "select name,algorithm, secret from tsigkeys"

	declare["get-all-domains-query"] = "select domains.id, domains.name, records.content, domains.type, domains.master, domains.notified_serial, domains.last_check, domains.account from domains LEFT JOIN records ON records.domain_id=domains.id AND records.type='SOA' AND records.name=domains.name WHERE records.disabled=0 OR :include_disabled"

	declare["list-comments-query"] = "SELECT domain_id,name,type,modified_at,account,comment FROM comments WHERE domain_id=:domain_id"
	declare["insert-comment-query"] = "INSERT INTO comments (domain_id, name, type, modified_at, account, comment) VALUES (:domain_id, :qname, :qtype, :modified_at, :account, :content)"
	declare["delete-comment-rrset-query"] = "DELETE FROM comments WHERE domain_id=:domain_id AND name=:qname AND type=:qtype"
	declare["delete-comments-query"] = "DELETE FROM comments WHERE domain_id=:domain_id"
	declare["search-records-query"] = record_query + " name LIKE :value ESCAPE '\\' OR content LIKE :value2 ESCAPE '\\' LIMIT :limit"
	declare["search-comments-query"] = "SELECT domain_id,name,type,modified_at,account,comment FROM comments WHERE name LIKE :value ESCAPE '\\' OR comment LIKE :value2 ESCAPE '\\' LIMIT :limit"
}

func Prepare(stmt string, args ...interface{}) (string, []interface{}, error) {
	if _, ok := declare[stmt]; !ok {
		return "", nil, errors.New("Нет информации о запросе: " + stmt)
	}
	qs, names, err := sql.CompileNamedQuery(declare[stmt], sql.QUESTION)
	if err != nil {
		return "", nil, errors.Wrap(err, "не удалось разобрать запрос")
	}
	arg, err := sql.ArgToMap(args...)
	if err != nil {
		return "", nil, errors.Wrap(err, "не удалось распределить аргументы по парам")
	}

	parametrs := make([]interface{}, 0, len(names))
	for _, name := range names {
		if value, ok := arg[name]; !ok {
			parametrs = append(parametrs, nil)
		} else {
			parametrs = append(parametrs, value)
		}
	}
	return qs, parametrs, nil
}
