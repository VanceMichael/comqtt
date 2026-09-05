// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 mochi-mqtt, mochi-co
// SPDX-FileContributor: mochi-co

package rest

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

const (
	MqttGetOverallPath       = "/api/v1/mqtt/stat/overall"
	MqttGetOnlinePath        = "/api/v1/mqtt/stat/online"
	MqttGetClientPath        = "/api/v1/mqtt/clients/{id}"
	MqttGetClientsPath       = "/api/v1/mqtt/clients"
	MqttGetSubscriptionsPath = "/api/v1/mqtt/subscriptions"
	MqttGetTopicsPath        = "/api/v1/mqtt/topics"
	MqttGetRetainedPath      = "/api/v1/mqtt/retained"
	MqttDelRetainedPath      = "/api/v1/mqtt/retained/{topic...}"
	MqttDisconnectClientPath = "/api/v1/mqtt/clients/{id}/disconnect"
	MqttGetBlacklistPath     = "/api/v1/mqtt/blacklist"
	MqttAddBlacklistPath     = "/api/v1/mqtt/blacklist/{id}"
	MqttDelBlacklistPath     = "/api/v1/mqtt/blacklist/{id}"
	MqttGetBansPath          = "/api/v1/mqtt/bans"
	MqttAddBanPath           = "/api/v1/mqtt/bans/{id}"
	MqttDelBanPath           = "/api/v1/mqtt/bans/{id}"
	MqttPublishMessagePath   = "/api/v1/mqtt/message"
	MqttGetConfigPath        = "/api/v1/mqtt/config"
	PrometheusMetrics        = "/metrics"
)

type subscriptionInfo struct {
	Filter string `json:"filter"`
	Count  int    `json:"count"`
}

type Handler = func(http.ResponseWriter, *http.Request)

type Rest struct {
	server *mqtt.Server
}

func New(server *mqtt.Server) *Rest {
	return &Rest{
		server: server,
	}
}

func (s *Rest) GenHandlers() map[string]Handler {
	return map[string]Handler{
		"GET " + MqttGetConfigPath:         s.viewConfig,
		"GET " + MqttGetOverallPath:        s.getOverallInfo,
		"GET " + MqttGetOnlinePath:         s.getOnlineCount,
		"GET " + MqttGetClientPath:         s.getClient,
		"GET " + MqttGetClientsPath:        s.getClients,
		"GET " + MqttGetSubscriptionsPath:  s.getSubscriptions,
		"GET " + MqttGetTopicsPath:         s.getTopics,
		"GET " + MqttGetRetainedPath:       s.getRetained,
		"DELETE " + MqttDelRetainedPath:    s.deleteRetained,
		"POST " + MqttDisconnectClientPath: s.disconnectClient,
		"GET " + MqttGetBlacklistPath:      s.blacklist,
		"POST " + MqttAddBlacklistPath:     s.kickClient,
		"DELETE " + MqttDelBlacklistPath:   s.blanchClient,
		"GET " + MqttGetBansPath:           s.getBans,
		"POST " + MqttAddBanPath:           s.addBan,
		"DELETE " + MqttDelBanPath:         s.delBan,
		"POST " + MqttPublishMessagePath:   s.publishMessage,
		"GET " + PrometheusMetrics: promhttp.HandlerFor(
			s.server.Options.PrometheusRegistry,
			promhttp.HandlerOpts{
				EnableOpenMetrics: false,
			}).ServeHTTP,
	}
}

func (s *Rest) getOverallInfo(w http.ResponseWriter, r *http.Request) {
	Ok(w, s.server.Info)
}

func (s *Rest) viewConfig(w http.ResponseWriter, r *http.Request) {
	Ok(w, s.server.Options)
}

func (s *Rest) getOnlineCount(w http.ResponseWriter, r *http.Request) {
	count := s.server.Info.ClientsConnected
	Ok(w, count)
}

func (s *Rest) getClient(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if cl, ol := s.server.Clients.Get(id); ol {
		Ok(w, genClient(cl))
	} else {
		Error(w, http.StatusNotFound, "client not found")
	}
}

func (s *Rest) getClients(w http.ResponseWriter, r *http.Request) {
	req := parsePagedRequest(r)
	search := req.Search
	req.Search = ""

	all := make([]client, 0, s.server.Clients.Len())
	for _, cl := range s.server.Clients.GetAll() {
		c := genClient(cl)
		if search != "" {
			sl := strings.ToLower(search)
			if !strings.Contains(strings.ToLower(c.ID), sl) &&
				!strings.Contains(strings.ToLower(c.Username), sl) {
				continue
			}
		}
		all = append(all, c)
	}

	Ok(w, Paginate(all, req))
}

func (s *Rest) getSubscriptions(w http.ResponseWriter, r *http.Request) {
	counts := make(map[string]int)
	for _, cl := range s.server.Clients.GetAll() {
		for filter := range cl.State.Subscriptions.GetAll() {
			counts[filter]++
		}
	}

	result := make([]subscriptionInfo, 0, len(counts))
	for filter, count := range counts {
		result = append(result, subscriptionInfo{Filter: filter, Count: count})
	}

	Ok(w, result)
}

func (s *Rest) getTopics(w http.ResponseWriter, r *http.Request) {
	Ok(w, s.server.Topics.WalkTopicTree())
}

func (s *Rest) getRetained(w http.ResponseWriter, r *http.Request) {
	req := parsePagedRequest(r)

	messages := make([]RetainedMsg, 0, s.server.Topics.Retained.Len())
	for _, pk := range s.server.Topics.Retained.GetAll() {
		if strings.HasPrefix(pk.TopicName, mqtt.SysPrefix) {
			continue
		}
		messages = append(messages, retainedMsgFromPacket(pk))
	}

	Ok(w, Paginate(messages, req))
}

func (s *Rest) disconnectClient(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cl, ok := s.server.Clients.Get(id)
	if !ok {
		Error(w, http.StatusNotFound, "client not found")
		return
	}

	s.server.DisconnectClient(cl, packets.ErrAdministrativeAction)
	Ok(w, map[string]string{"id": id})
}

func (s *Rest) deleteRetained(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	s.server.Topics.Retained.Delete(topic)
	Ok(w, map[string]string{"topic": topic})
}

func (s *Rest) publishMessage(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	var msg message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		Error(w, http.StatusBadRequest, err.Error())
		return
	}

	if err := s.server.Publish(msg.TopicName, []byte(msg.Payload), msg.Retain, msg.Qos); err != nil {
		Error(w, http.StatusInternalServerError, err.Error())
	} else {
		Ok(w, msg)
	}
}

func (s *Rest) kickClient(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("id")
	s.server.BlacklistMutexLock()
	if !slices.Contains(s.server.Blacklist, cid) {
		s.server.Blacklist = append(s.server.Blacklist, cid)
	}
	s.server.BlacklistMutexUnlock()
	if cl, ol := s.server.Clients.Get(cid); ol {
		s.server.DisconnectClient(cl, packets.ErrNotAuthorized)
		Ok(w, cid)
	} else {
		Error(w, http.StatusNotFound, "client not found")
	}
}

func (s *Rest) blanchClient(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("id")
	s.server.BlacklistMutexLock()
	idx := -1
	for i, v := range s.server.Blacklist {
		if v == cid {
			idx = i
			break
		}
	}
	if idx >= 0 {
		s.server.Blacklist = append(s.server.Blacklist[:idx], s.server.Blacklist[idx+1:]...)
	}
	ok := idx >= 0
	s.server.BlacklistMutexUnlock()
	if ok {
		Ok(w, cid)
	}
}

func (s *Rest) blacklist(w http.ResponseWriter, r *http.Request) {
	s.server.BlacklistMutexLock()
	defer s.server.BlacklistMutexUnlock()
	if s.server.Blacklist == nil {
		Error(w, http.StatusNotFound, "blacklist not found")
	} else {
		Ok(w, s.server.Blacklist)
	}
}

// banRequest is the optional body of POST /bans/{id}. An empty body or
// ttl_seconds == 0 creates a permanent ban; positive ttl_seconds creates a
// temporary one that expires automatically.
type banRequest struct {
	TTLSeconds int64  `json:"ttl_seconds"`
	Reason     string `json:"reason"`
}

// getBans lists active ban policies (temporary and permanent) with the
// remaining time of each temporary ban.
// GET /api/v1/mqtt/bans
func (s *Rest) getBans(w http.ResponseWriter, r *http.Request) {
	Ok(w, s.server.BanList())
}

// addBan bans a client id for ttl_seconds (0 = permanent), immediately
// disconnecting the client if it is connected to this node.
// POST /api/v1/mqtt/bans/{id}
func (s *Rest) addBan(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("id")
	if cid == "" {
		Error(w, http.StatusBadRequest, "client id is required")
		return
	}

	var req banRequest
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && err != io.EOF {
			Error(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if req.TTLSeconds < 0 {
		Error(w, http.StatusBadRequest, "ttl_seconds must be greater than or equal to 0")
		return
	}

	policy := s.server.BanClient(cid, req.Reason, time.Duration(req.TTLSeconds)*time.Second)
	disconnected := false
	if cl, ol := s.server.Clients.Get(cid); ol {
		s.server.DisconnectClient(cl, packets.ErrNotAuthorized)
		disconnected = true
	}
	Ok(w, map[string]any{"ban": policy, "disconnected": disconnected})
}

// delBan removes the ban policy for a client id.
// DELETE /api/v1/mqtt/bans/{id}
func (s *Rest) delBan(w http.ResponseWriter, r *http.Request) {
	cid := r.PathValue("id")
	if cid == "" {
		Error(w, http.StatusBadRequest, "client id is required")
		return
	}
	if ok := s.server.UnbanClient(cid); !ok {
		Error(w, http.StatusNotFound, "ban not found")
		return
	}
	Ok(w, map[string]string{"id": cid})
}

func parsePagedRequest(r *http.Request) PagedRequest {
	req := PagedRequest{Page: 1, PageSize: defaultPageSize}
	req.Search = r.URL.Query().Get("search")
	if v, err := strconv.Atoi(r.URL.Query().Get("page")); err == nil && v > 0 {
		req.Page = v
	}
	if v, err := strconv.Atoi(r.URL.Query().Get("page_size")); err == nil && v > 0 {
		req.PageSize = v
	}
	return req
}
