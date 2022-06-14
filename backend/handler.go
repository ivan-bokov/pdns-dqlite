package backend

import (
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/ivan-bokov/pdns-dqlite/backend/core"
)

type Handler struct {
	svc *core.Service
}

func New(svc *core.Service) *Handler {
	return &Handler{svc: svc}
}
func (h *Handler) noImplementation(g *gin.Context) {
	g.JSON(200, gin.H{"result": false})
}
func (h *Handler) InitRoutes() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()

	r.GET("lookup/:qname/:qtype", h.lookup)    // ++++
	r.GET("list/:domain_id/:zonename", h.list) // ++++
	r.GET("getbeforeandafternamesabsolute/:domain_id/:qname", h.getbeforeandafternamesabsolute)
	r.GET("getalldomainmetadata/:name", h.getAllDomainMetadata)    // ++++
	r.GET("getdomainmetadata/:name/:kind", h.getDomainMetadata)    // ++++
	r.PATCH("setdomainmetadata/:name/:kind", h.setDomainMetadata)  // ++++
	r.GET("getdomainkeys/:name", h.getDomainKeys)                  // ++++
	r.PUT("adddomainkey/:name", h.addDomainKey)                    // +++?
	r.DELETE("removedomainkey/:name/:id", h.removeDomainKey)       // ++++
	r.POST("activatedomainkey/:name/:id", h.activateDomainKey)     // ++++
	r.POST("deactivatedomainkey/:name/:id", h.deactivateDomainKey) // ++++
	r.POST("publishdomainkey/:name/:id", h.publishDomainKey)       // ++++
	r.POST("unpublishdomainkey/:name/:id", h.unpublishDomainKey)   // ++++
	r.GET("gettsigkey/:name", h.getTSIGKey)                        // ++++
	r.GET("getdomaininfo/:name", h.getDomainInfo)                  // ++++
	r.PATCH("setnotified/:id", h.setNotified)                      // ++++
	r.GET("isMaster/:name/:ip", h.noImplementation)
	r.POST("supermasterbackend/:ip/:domain", h.superMasterBackend)    // ++++
	r.POST("createslavedomain/:ip/:domain", h.createSlaveDomain)      // ++++
	r.PATCH("replacerrset/:domain_id/:qname/:qtype", h.replaceRRSet)  // ++++
	r.PATCH("feedrecord/:trxid", h.feedRecord)                        // ++--
	r.PATCH("feedents/:domain_id", h.feedents)                        // ++++
	r.PATCH("feedEnts3/:domain_id/:domain", h.feedents3)              // ++++
	r.POST("starttransaction/:domain_id/:domain", h.startTransaction) // ++++
	r.POST("committransaction/:trxid", h.commitTransaction)           // ++++
	r.POST("aborttransaction/:trxid", h.abortTransaction)             // ++++
	r.POST("calculatesoaserial/:domain", h.noImplementation)
	r.POST("directBackendCmd", h.noImplementation)
	r.GET("getAllDomains", h.getAllDomains)         // ++++
	r.GET("searchRecords", h.searchRecords)         // ++++
	r.GET("getUpdatedMasters", h.getUpdatedMasters) // ++++
	r.GET("getUnfreshSlaveInfos", h.noImplementation)
	r.PATCH("setFresh/:id", h.setFresh) // ++++

	r.GET("test/:key", h.getTest)
	r.POST("test/:key", h.postTest)

	return r
}
func (h *Handler) getTest(g *gin.Context) {
	key := g.Param("key")
	val, err := h.svc.GetTest(key)
	if err != nil {
		g.JSON(204, gin.H{"err": err.Error()})
	}
	g.JSON(200, val)
}
func (h *Handler) postTest(g *gin.Context) {
	key := g.Param("key")
	value, _ := g.GetQuery("value")
	err := h.svc.PostTest(key, value)
	if err != nil {
		g.JSON(204, gin.H{"err": err.Error()})
	}
	g.JSON(200, "OK")
}
func (h *Handler) getUpdatedMasters(g *gin.Context) {
	di, err := h.svc.GetUpdatedMasters()
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(http.StatusBadRequest, gin.H{"result": di})
}
func (h *Handler) searchRecords(g *gin.Context) {
	var maxResult int
	var err error
	if maxResultS, ok := g.GetQuery("maxResults"); ok {
		maxResult, err = strconv.Atoi(maxResultS)
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	pattern, _ := g.GetQuery("pattern")
	rr, err := h.svc.SearchRecords(pattern, maxResult)
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(http.StatusBadRequest, gin.H{"result": rr})
}
func (h *Handler) abortTransaction(g *gin.Context) {
	var trxid int
	var err error
	if g.Param("trxid") != "" {
		trxid, err = strconv.Atoi(g.Param("trxid"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	err = h.svc.AbortTransaction(trxid)
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(http.StatusBadRequest, gin.H{"result": true})
}
func (h *Handler) commitTransaction(g *gin.Context) {
	var trxid int
	var err error
	if g.Param("trxid") != "" {
		trxid, err = strconv.Atoi(g.Param("trxid"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	err = h.svc.CommitTransaction(trxid)
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(http.StatusBadRequest, gin.H{"result": true})
}
func (h *Handler) startTransaction(g *gin.Context) {
	var trxid int
	var err error
	var domainID int
	if trx, ok := g.GetPostForm("trxid"); ok {
		trxid, err = strconv.Atoi(trx)
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	if g.Param("domain_id") != "" {
		domainID, err = strconv.Atoi(g.Param("domain_id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	err = h.svc.StartTransaction(trxid, domainID)
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(http.StatusBadRequest, gin.H{"result": true})
}

func (h *Handler) feedents3(g *gin.Context) {
	domain := g.Param("domain")
	var domainID int
	var trxid int
	var narrow bool
	var err error
	if trx, ok := g.GetPostForm("trxid"); ok {
		trxid, err = strconv.Atoi(trx)
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	if narrowS, ok := g.GetPostForm("narrow"); ok {
		narrow, err = strconv.ParseBool(narrowS)
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	if g.Param("domain_id") != "" {
		domainID, err = strconv.Atoi(g.Param("domain_id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	nonterms := map[string]bool{}

	if nnt, ok := g.GetPostFormArray("nonterm"); ok {
		for _, v := range nnt {
			nonterms[v] = true
		}
	}
	err = h.svc.FeedEnts3(trxid, domainID, domain, nonterms, narrow)
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(http.StatusBadRequest, gin.H{"result": true})
}

func (h *Handler) feedents(g *gin.Context) {
	var domainID int
	var trxid int
	var err error
	if trx, ok := g.GetPostForm("trxid"); ok {
		trxid, err = strconv.Atoi(trx)
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	if g.Param("domain_id") != "" {
		domainID, err = strconv.Atoi(g.Param("domain_id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	nonterms := map[string]bool{}

	if nnt, ok := g.GetPostFormArray("nonterm"); ok {
		for _, v := range nnt {
			nonterms[v] = true
		}
	}
	err = h.svc.FeedEnts(trxid, domainID, nonterms)
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(http.StatusBadRequest, gin.H{"result": true})
}
func (h *Handler) replaceRRSet(g *gin.Context) {
	qname := g.Param("qname")
	qtype := g.Param("qtype")
	var domainID int
	var trxid int
	rrset := make([]*core.DNSResourceRecord, 0, 10)
	var err error
	if g.Param("domain_id") != "" {
		domainID, err = strconv.Atoi(g.Param("domain_id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	if trx, ok := g.GetPostForm("trxid"); ok {
		trxid, err = strconv.Atoi(trx)
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	if m, ok := g.GetPostFormMap("rrset"); ok {
		var ttl int
		var err error
		if _, ok := m["ttl"]; ok {
			ttl, err = strconv.Atoi(m["ttl"])
			if err != nil {
				g.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"result": false})
				return
			}
		}

		rr := &core.DNSResourceRecord{
			Qname:        m["qname"],
			OrderName:    m["order_name"],
			WildcardName: m["wildcard_name"],
			Content:      m["content"],
			TTL:          ttl,
			Qtype:        m["qtype"],
			Auth:         m["auth"] == "1",
			Qclass:       m["qclass"],
		}
		rrset = append(rrset, rr)
	}
	err = h.svc.ReplaceRRSet(trxid, domainID, qname, qtype, rrset)
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(http.StatusBadRequest, gin.H{"result": true})

}

func (h *Handler) superMasterBackend(g *gin.Context) {
	ip := g.Param("ip")
	domain := g.Param("domain")
	type NSSet struct {
		Row interface{} `json:"nsset" form:"nsset""`
	}
	nsset := new(NSSet)
	if ok := g.Bind(nsset); ok != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	// TODO: Не корректно парсится форма
	nn := make([]*core.DNSResourceRecord, 0)
	//	for _, v := range nsset.Row {
	//		nn = append(nn, v)
	//	}
	ns, account, err := h.svc.SuperMasterBackend(ip, domain, nn)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": gin.H{"account": account, "nameserver": ns}})
}

func (h *Handler) getTSIGKey(g *gin.Context) {
	name := g.Param("name")
	alg, content, err := h.svc.GetTSIGKey(name)
	if err != nil || content == nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": gin.H{"algorithm": alg, "content": content}})
}
func (h *Handler) removeDomainKey(g *gin.Context) {
	name := g.Param("name")
	var keyID int
	var err error
	if g.Param("id") != "" {
		keyID, err = strconv.Atoi(g.Param("id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	err = h.svc.RemoveDomainKey(name, keyID)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}
func (h *Handler) activateDomainKey(g *gin.Context) {
	name := g.Param("name")
	var keyID int
	var err error
	if g.Param("id") != "" {
		keyID, err = strconv.Atoi(g.Param("id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	err = h.svc.ActivateDomainKey(name, keyID)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}
func (h *Handler) deactivateDomainKey(g *gin.Context) {
	name := g.Param("name")
	var keyID int
	var err error
	if g.Param("id") != "" {
		keyID, err = strconv.Atoi(g.Param("id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	err = h.svc.DeactivateDomainKey(name, keyID)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}
func (h *Handler) publishDomainKey(g *gin.Context) {
	name := g.Param("name")
	var keyID int
	var err error
	if g.Param("id") != "" {
		keyID, err = strconv.Atoi(g.Param("id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	err = h.svc.PublishDomainKey(name, keyID)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}
func (h *Handler) unpublishDomainKey(g *gin.Context) {
	name := g.Param("name")
	var keyID int
	var err error
	if g.Param("id") != "" {
		keyID, err = strconv.Atoi(g.Param("id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	err = h.svc.UnPublishDomainKey(name, keyID)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}

func (h *Handler) getDomainKeys(g *gin.Context) {
	name := g.Param("name")
	keys, err := h.svc.GetDomainKeys(name)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": keys})
}

func (h *Handler) getAllDomains(g *gin.Context) {
	disabled := false
	var err error
	if g.Query("includeDisabled") != "" {
		disabled, err = strconv.ParseBool(g.Query("includeDisabled"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	di, err := h.svc.GetAllDomains(disabled)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": di})
}

func (h *Handler) lookup(g *gin.Context) {
	qtype := g.Param("qtype")
	qname := g.Param("qname")
	zoneID := -1
	var err error
	if g.Request.Header.Get("X-RemoteBackend-zone-id") != "" {
		zoneID, err = strconv.Atoi(g.Request.Header.Get("X-RemoteBackend-zone-id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	listRR, err := h.svc.Lookup(qtype, qname, zoneID)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": listRR})
}
func (h *Handler) getDomainInfo(g *gin.Context) {
	name := g.Param("name")
	di, err := h.svc.GetDomainInfo(name)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": di})
}

func (h *Handler) list(g *gin.Context) {
	zonename := g.Param("zonename")
	domainID := -1
	var err error
	if g.Request.Header.Get("X-RemoteBackend-domain-id") != "" {
		domainID, err = strconv.Atoi(g.Request.Header.Get("X-RemoteBackend-domain-id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	if g.Param("domain_id") != "" {
		domainID, err = strconv.Atoi(g.Param("domain_id"))
		if err != nil {
			g.JSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	listRR, err := h.svc.List(zonename, domainID, false)
	if err != nil {
		g.JSON(200, gin.H{"result": make([]string, 0)})
		return
	}
	g.JSON(200, gin.H{"result": listRR})
}
func (h *Handler) getAllDomainMetadata(g *gin.Context) {
	name := g.Param("name")
	var err error
	meta, err := h.svc.GetAllDomainMetadata(name)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": meta})
}

func (h *Handler) getbeforeandafternamesabsolute(g *gin.Context) {
	//TODO непонятно что делать с параметрами, разобраться когда все закончу либо осенит

}

func (h *Handler) setDomainMetadata(g *gin.Context) {
	name := g.Param("name")
	kind := g.Param("kind")
	type valueMetadata struct {
		Value []string `json:"value,omitempty" form:"value"`
	}
	values := new(valueMetadata)
	if err := g.Bind(values); err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	err := h.svc.SetDomainMetadata(name, kind, values.Value)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}
func (h *Handler) getDomainMetadata(g *gin.Context) {
	name := g.Param("name")
	kind := g.Param("kind")
	meta, err := h.svc.GetDomainMetadata(name, kind)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": meta})
}

func (h *Handler) addDomainKey(g *gin.Context) {
	name := g.Param("name")
	key := new(core.KeyData)
	var err error
	if flags, ok := g.GetPostForm("flags"); ok {
		key.Flags, err = strconv.Atoi(flags)
		if err != nil {
			g.AbortWithStatus(http.StatusBadRequest)
			return
		}
	}
	if active, ok := g.GetPostForm("active"); ok {
		key.Active, err = strconv.ParseBool(active)
		if err != nil {
			g.AbortWithStatus(http.StatusBadRequest)
			return
		}
	}
	if published, ok := g.GetPostForm("published"); ok {
		key.Published, err = strconv.ParseBool(published)
		if err != nil {
			g.AbortWithStatus(http.StatusBadRequest)
			return
		}
	}
	if content, ok := g.GetPostForm("content"); ok {
		key.Content = content
	}

	err = h.svc.AddDomainKey(name, key)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}

func (h *Handler) feedRecord(g *gin.Context) {
	m := make(map[string]string)
	var ok bool
	if m, ok = g.GetPostFormMap("rr"); !ok {
		g.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	var ttl int
	var err error
	if _, ok := m["ttl"]; ok {
		ttl, err = strconv.Atoi(m["ttl"])
		if err != nil {
			g.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"result": false})
			return
		}
	}
	var auth bool
	if auth, err = strconv.ParseBool(m["auth"]); err != nil {
		g.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	err = h.svc.FeedRecord(&core.DNSResourceRecord{
		Qname:   m["qname"],
		Content: m["content"],
		TTL:     ttl,
		Qtype:   m["qtype"],
		Auth:    auth,
		Qclass:  m["qclass"],
	}, "")
	if err != nil {
		g.AbortWithStatusJSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}

func (h *Handler) createSlaveDomain(g *gin.Context) {
	ip := g.Param("ip")
	domain := g.Param("domain")
	err := h.svc.CreateSlaveDomain(ip, domain)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}

func (h *Handler) setFresh(g *gin.Context) {
	id, err := strconv.Atoi(g.Param("id"))
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	err = h.svc.SetFresh(id)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}

func (h *Handler) setNotified(g *gin.Context) {
	id, err := strconv.Atoi(g.Param("id"))
	if err != nil {
		g.JSON(http.StatusBadRequest, gin.H{"result": false})
		return
	}
	var serial int
	if s, ok := g.GetPostForm("serial"); ok {
		serial, err = strconv.Atoi(s)
		if err != nil {
			g.AbortWithStatus(http.StatusBadRequest)
			return
		}
	}
	err = h.svc.SetNotified(id, serial)
	if err != nil {
		g.JSON(200, gin.H{"result": false})
		return
	}
	g.JSON(200, gin.H{"result": true})
}
