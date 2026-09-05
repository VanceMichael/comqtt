// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 wind
// SPDX-FileContributor: wind (573966@qq.com)

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/wind-c/comqtt/v2/cluster/discovery"
	"github.com/wind-c/comqtt/v2/cluster/discovery/mlist"
	"github.com/wind-c/comqtt/v2/cluster/discovery/serf"
	"github.com/wind-c/comqtt/v2/cluster/log"
	"github.com/wind-c/comqtt/v2/cluster/message"
	"github.com/wind-c/comqtt/v2/cluster/raft"
	"github.com/wind-c/comqtt/v2/cluster/raft/etcd"
	"github.com/wind-c/comqtt/v2/cluster/raft/hashicorp"
	"github.com/wind-c/comqtt/v2/cluster/topics"
	"github.com/wind-c/comqtt/v2/cluster/utils"
	"github.com/wind-c/comqtt/v2/config"
	"github.com/wind-c/comqtt/v2/mqtt"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

const (
	DirectionInbound byte = iota
	DirectionOutbound
)

const (
	NodesFile = "nodes.json"
	PeersFile = "peers.json"
)

type Agent struct {
	membership        discovery.Node
	ctx               context.Context
	cancel            context.CancelFunc
	Config            *config.Cluster
	mqttServer        *mqtt.Server
	grpcService       *RpcService
	grpcClientManager *ClientManager
	raftPool          *ants.Pool
	OutPool           *ants.Pool
	inPool            *ants.Pool
	subTree           *topics.Index
	raftPeer          raft.IPeer
	raftNotifyCh      chan *message.Message
	inboundMsgCh      chan []byte
	grpcMsgCh         chan *message.Message
}

func NewAgent(conf *config.Cluster) *Agent {
	ctx, cancel := context.WithCancel(context.Background())
	return &Agent{
		ctx:          ctx,
		cancel:       cancel,
		Config:       conf,
		subTree:      topics.New(),
		raftNotifyCh: make(chan *message.Message, 1024),
		inboundMsgCh: make(chan []byte, 10240),
		grpcMsgCh:    make(chan *message.Message, 10240),
	}
}

func (a *Agent) Start() (err error) {
	// setup raft
	if a.Config.RaftPort == 0 || a.Config.DiscoveryWay == config.DiscoveryWayMemberlist {
		a.Config.RaftPort = mlist.GetRaftPortFromBindPort(a.Config.BindPort)
	}
	if a.Config.RaftDir == "" {
		a.Config.RaftDir = path.Join("data", a.Config.NodeName)
	}

	// listen for raft apply notifications
	go a.raftApplyListener()
	// leader purges expired temporary ban generations through raft
	go a.banSweeper()

	if a.Config.RaftImpl == config.RaftImplEtcd {
		if a.raftPeer, err = etcd.Setup(a.Config, a.raftNotifyCh); err != nil {
			return
		}
	} else {
		if a.raftPeer, err = hashicorp.Setup(a.Config, a.raftNotifyCh); err != nil {
			return
		}
	}
	raftAddr := net.JoinHostPort(a.Config.BindAddr, strconv.Itoa(a.Config.RaftPort))
	OnJoinLog(a.Config.NodeName, raftAddr, "setup raft", nil)

	// create and join cluster
	if utils.PathExists(a.getNodesFile()) {
		ms := discovery.GenMemberAddrs(discovery.ReadMembers(a.getNodesFile()))
		if ms != nil {
			a.Config.Members = ms
		}
	}

	switch a.Config.DiscoveryWay {
	case config.DiscoveryWayMemberlist:
		a.membership = mlist.New(a.Config, a.inboundMsgCh)
	default:
		a.membership = serf.New(a.Config, a.inboundMsgCh)
	}

	a.membership.BindMqttServer(a.mqttServer)
	if err := a.membership.Setup(); err != nil {
		return err
	}

	// start grpc server
	if a.Config.GrpcEnable {
		a.grpcService = NewRpcService(a)
		a.grpcClientManager = NewClientManager(a)
		if err := a.grpcService.StartRpcServer(); err != nil {
			return err
		}
		log.Info("grpc listen at", "addr", net.JoinHostPort(a.Config.BindAddr, strconv.Itoa(a.Config.GrpcPort)))
	}

	// init goroutine pool
	a.initPool()

	// process node event
	go a.processNodeEvent()

	return nil
}

func (a *Agent) initPool() error {
	var err error

	// create raft app goroutine pool and raft msg
	if a.raftPool, err = ants.NewPool(0, ants.WithNonblocking(true)); err != nil {
		return err
	}

	// create outbound goroutine pool and outbound msg
	if a.OutPool, err = ants.NewPool(a.Config.OutboundPoolSize, ants.WithNonblocking(a.Config.InoutPoolNonblocking)); err != nil {
		return err
	}

	// create inbound goroutine pool and process inbound msg
	if a.inPool, err = ants.NewPool(a.Config.InboundPoolSize, ants.WithNonblocking(a.Config.InoutPoolNonblocking)); err != nil {
		return err
	}

	// start incoming message receiving goroutine
	go a.processInboundMsg()

	return nil
}

func (a *Agent) Stop() {
	a.cancel()
	a.OutPool.Release()
	a.raftPool.Release()
	if a.inPool != nil {
		a.inPool.Release()
	}

	// stop raft
	log.Info("stopping raft...")
	a.raftPeer.Stop()
	log.Info("raft stopped")

	// stop node
	log.Info("stopping node...")
	a.membership.Stop()
	a.grpcService.StopRpcServer()
	log.Info("grpc server stopped")
	log.Info("node stopped")
}

func (a *Agent) BindMqttServer(server *mqtt.Server) {
	server.AddHook(new(MqttEventHook), a)
	a.mqttServer = server
}

func (a *Agent) GetLocalName() string {
	return a.Config.NodeName
}

func (a *Agent) getNodeMember(nodeId string) *discovery.Member {
	for _, m := range a.membership.Members() {
		if m.Name == nodeId {
			return &m
		}
	}
	return nil
}

func (a *Agent) GetMemberList() []discovery.Member {
	return a.membership.Members()
}

// Leader returns the node id of the current Raft leader, or "" if no leader
// is elected (e.g. mid-election or before the cluster has converged).
//
// Exposed for monitoring tooling and external hook authors that need to
// render cluster topology - the dashboard's cluster page in particular.
// Trivial wrapper over raftPeer.GetLeader(), which is otherwise unreachable
// from outside this package.
func (a *Agent) Leader() string {
	_, id := a.raftPeer.GetLeader()
	return id
}

func getRaftPeerAddr(member *discovery.Member) string {
	// using serf
	if raftPort, ok := member.Tags[discovery.TagRaftPort]; ok {
		return net.JoinHostPort(member.Addr, raftPort)
	}

	// using memberlist
	return net.JoinHostPort(member.Addr, strconv.Itoa(mlist.GetRaftPortFromBindPort(member.Port)))
}

func getGrpcAddr(member *discovery.Member) string {
	// using serf
	if grpcPort, ok := member.Tags[discovery.TagGrpcPort]; ok {
		return net.JoinHostPort(member.Addr, grpcPort)
	}

	// using memberlist
	return net.JoinHostPort(member.Addr, strconv.Itoa(mlist.GetGRPCPortFromBindPort(member.Port)))
}

func (a *Agent) Stat() map[string]int64 {
	return a.membership.Stat()
}

func (a *Agent) raftApplyListener() {
	for {
		select {
		case msg := <-a.raftNotifyCh:
			// ban notifications are consumed on every node, including the
			// proposer: the local mirror must be updated and the banned
			// client's live connection on this node must be torn down.
			switch msg.Type {
			case message.BanAdd:
				a.applyBanAdd(msg)
				continue
			case message.BanDel:
				a.applyBanDel(msg)
				continue
			}
			if msg.NodeID == "" || msg.NodeID == a.GetLocalName() || len(msg.Payload) == 0 {
				continue
			}
			filter := string(msg.Payload)
			if msg.Type == packets.Subscribe {
				a.subTree.Subscribe(filter)
			} else if msg.Type == packets.Unsubscribe {
				a.subTree.Unsubscribe(filter)
			} else {
				continue
			}
		case <-a.ctx.Done():
			return
		}
	}
}

// applyBanAdd mirrors a raft-committed ban into the local server book and
// immediately disconnects the client if it is connected to this node.
func (a *Agent) applyBanAdd(msg *message.Message) {
	var ban raft.Ban
	if err := json.Unmarshal(msg.Payload, &ban); err != nil {
		log.Error("apply ban unmarshal", "error", err)
		return
	}
	a.mqttServer.ApplyBan(ban.ClientID, ban.Reason, ban.CreatedAt, ban.ExpiresAt)
	if cl, ok := a.mqttServer.Clients.Get(ban.ClientID); ok {
		if err := a.mqttServer.DisconnectClient(cl, packets.ErrNotAuthorized); err != nil {
			log.Debug("disconnect banned client", "cid", ban.ClientID, "error", err)
		}
		log.Info("ban disconnect client", "cid", ban.ClientID)
	}
}

// applyBanDel mirrors a raft-committed ban removal into the local server book.
func (a *Agent) applyBanDel(msg *message.Message) {
	var rm raft.BanRemoval
	if err := json.Unmarshal(msg.Payload, &rm); err != nil {
		log.Error("apply unban unmarshal", "error", err)
		return
	}
	if rm.CreatedAt > 0 && rm.ExpiresAt > 0 {
		a.mqttServer.RemoveBanGeneration(rm.ClientID, rm.CreatedAt, rm.ExpiresAt)
	} else {
		a.mqttServer.ApplyBanRemoval(rm.ClientID, rm.At)
	}
}

// send the message to the leader apply
func (a *Agent) raftPropose(msg *message.Message) {
	if a.raftPeer.IsApplyRight() {
		err := a.raftPeer.Propose(msg)
		OnApplyLog(a.GetLocalName(), msg.NodeID, msg.Type, msg.Payload, "raft apply log", err)
	} else { //send to leader apply
		_, leaderId := a.raftPeer.GetLeader()
		if leaderId == "" {
			if a.Config.GrpcEnable {
				a.grpcClientManager.RaftApplyToOthers(msg)
			} else {
				a.membership.SendToOthers(msg.MsgpackBytes())
			}
			OnApplyLog("unknown", msg.NodeID, msg.Type, msg.Payload, "raft broadcast log", nil)
		} else {
			if a.Config.GrpcEnable {
				a.grpcClientManager.RelayRaftApply(leaderId, msg)
			} else {
				a.membership.SendToNode(leaderId, msg.MsgpackBytes())
			}
			OnApplyLog(leaderId, msg.NodeID, msg.Type, msg.Payload, "raft forward log", nil)
		}
	}
}

func (a *Agent) getNodesFile() string {
	if a.Config.NodesFileDir != "" {
		return filepath.Join(a.Config.NodesFileDir, a.Config.NodeName+"-"+NodesFile)
	}
	return a.Config.NodeName + "-" + NodesFile
}

func (a *Agent) getPeersFile() string {
	return filepath.Join(a.Config.RaftDir, PeersFile)
}

func (a *Agent) genNodesFile() {
	if err := discovery.GenNodesFile(a.getNodesFile(), a.membership.Members()); err != nil {
		log.Error("gen nodes file", "error", err)
	}
	if err := a.raftPeer.GenPeersFile(a.getPeersFile()); err != nil {
		log.Error("gen peers file", "error", err)
	}
}

// processNodeEvent handle events of node join and leave
func (a *Agent) processNodeEvent() {
	for {
		select {
		case event := <-a.membership.EventChan():
			var err error
			prompt := "raft join"
			nodeName := event.Name
			addr := getRaftPeerAddr(&event.Member)
			//addr := event.Addr
			if event.Type == discovery.EventJoin {
				if nodeName != a.GetLocalName() && a.raftPeer.IsApplyRight() {
					err = a.raftPeer.Join(nodeName, addr)
					prompt = "raft join"
				}
			} else if event.Type == discovery.EventLeave {
				err = a.raftPeer.Leave(nodeName)
				if a.Config.GrpcEnable {
					a.grpcClientManager.RemoveGrpcClient(nodeName)
				}
				prompt = "raft leave"
			} else {
				prompt = "raft update"
			}
			OnJoinLog(nodeName, addr, prompt, err)
			go a.genNodesFile()
		case <-a.ctx.Done():
			return
		}
	}
}

func (a *Agent) processRelayMsg(msg *message.Message) {
	switch msg.Type {
	case message.RaftJoin:
		addr := string(msg.Payload)
		err := a.raftPeer.Join(msg.NodeID, addr)
		OnJoinLog(msg.NodeID, addr, "raft join", err)
	case packets.Subscribe, packets.Unsubscribe, message.BanAdd, message.BanDel:
		a.raftPropose(msg)
	case packets.Publish:
		pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Publish}}
		pk.ProtocolVersion = msg.ProtocolVersion
		pk.Origin = msg.ClientID
		if err := a.readFixedHeader(msg.Payload, &pk.FixedHeader); err != nil {
			return
		}
		offset := len(msg.Payload) - pk.FixedHeader.Remaining          // Unpack fixedheader.
		if err := pk.PublishDecode(msg.Payload[offset:]); err == nil { // Unpack skips fixedheader
			a.mqttServer.PublishToSubscribers(pk, false)
			OnPublishPacketLog(DirectionInbound, msg.NodeID, msg.ClientID, pk.TopicName, pk.PacketID)
		}
	case packets.Connect:
		//If a client is connected to another node, the client's data cached on the node needs to be cleared
		if existing, ok := a.mqttServer.Clients.Get(msg.ClientID); ok {
			// connection notify from other node
			existing.Stop(packets.ErrSessionTakenOver)
			// clean local session and subscriptions
			a.mqttServer.UnsubscribeClient(existing)
			a.mqttServer.Clients.Delete(msg.ClientID)
		}
		OnConnectPacketLog(DirectionInbound, msg.NodeID, msg.ClientID)
	default:
		log.Warn("unhandled default case")
	}
}

func (a *Agent) readFixedHeader(b []byte, fh *packets.FixedHeader) error {
	err := fh.Decode(b[0])
	if err != nil {
		return err
	}

	fh.Remaining, _, err = packets.DecodeLength(bytes.NewReader(b[1:]))
	if err != nil {
		return err
	}

	return nil
}

// ProcessInboundMsg process messages from other nodes in the cluster
func (a *Agent) processInboundMsg() {
	for {
		select {
		case <-a.ctx.Done():
			return
		case bs := <-a.inboundMsgCh:
			a.inPool.Submit(func() {
				var msg message.Message
				if err := msg.MsgpackLoad(bs); err == nil {
					a.processRelayMsg(&msg)
				}
			})
		case msg := <-a.grpcMsgCh:
			a.inPool.Submit(func() {
				a.processRelayMsg(msg)
			})
		}
	}
}

func (a *Agent) SubmitOutPublishTask(pk *packets.Packet, sharedFilters map[string]bool) {
	a.OutPool.Submit(func() {
		a.processOutboundPublish(pk, sharedFilters)
	})
}

func (a *Agent) SubmitOutConnectTask(pk *packets.Packet) {
	a.OutPool.Submit(func() {
		a.processOutboundConnect(pk)
	})
}

func (a *Agent) SubmitRaftTask(msg *message.Message) {
	a.raftPool.Submit(func() {
		a.raftPropose(msg)
	})
}

// processOutboundPublish process outbound publish msg
func (a *Agent) processOutboundPublish(pk *packets.Packet, sharedFilters map[string]bool) {
	msg := message.Message{
		NodeID:          a.Config.NodeName,
		ClientID:        pk.Origin,
		ProtocolVersion: pk.ProtocolVersion,
	}

	var buf bytes.Buffer
	pk.Mods.AllowResponseInfo = true
	if err := pk.PublishEncode(&buf); err != nil {
		return
	}
	msg.Type = packets.Publish
	msg.Payload = buf.Bytes()
	tmpFilters := a.subTree.Scan(pk.TopicName, make([]string, 0))
	oldNodes := make([]string, 0)
	filters := make([]string, 0)
	for _, filter := range tmpFilters {
		if !utils.Contains(filters, filter) {
			filters = append(filters, filter)
		}
	}
	for _, filter := range filters {
		ns := a.pickNodes(filter, sharedFilters)
		for _, node := range ns {
			if node != a.GetLocalName() && !utils.Contains(oldNodes, node) {
				if a.Config.GrpcEnable {
					a.grpcClientManager.RelayPublishPacket(node, &msg)
				} else {
					bs := msg.MsgpackBytes()
					a.membership.SendToNode(node, bs)
				}
				oldNodes = append(oldNodes, node)
				OnPublishPacketLog(DirectionOutbound, node, pk.Origin, pk.TopicName, pk.PacketID)
			}
		}
	}
}

// processOutboundConnect process outbound connect msg
func (a *Agent) processOutboundConnect(pk *packets.Packet) {
	msg := message.Message{
		NodeID:          a.Config.NodeName,
		ClientID:        pk.Origin,
		ProtocolVersion: pk.ProtocolVersion,
	}

	msg.Type = packets.Connect
	if msg.ClientID == "" {
		msg.ClientID = pk.Connect.ClientIdentifier
	}
	if a.Config.GrpcEnable {
		a.grpcClientManager.ConnectNotifyToOthers(&msg)
	} else {
		a.membership.SendToOthers(msg.MsgpackBytes())
	}
	OnConnectPacketLog(DirectionOutbound, a.GetLocalName(), msg.ClientID)
}

// pickNodes pick nodes, if the filter is shared, select a node at random
func (a *Agent) pickNodes(filter string, sharedFilters map[string]bool) (ns []string) {
	tmpNs := a.raftPeer.Lookup(filter)
	if tmpNs == nil || len(tmpNs) == 0 {
		return ns
	}

	if strings.HasPrefix(filter, topics.SharePrefix) {
		if b, ok := sharedFilters[filter]; ok && b {
			return ns
		}

		for _, n := range tmpNs {
			// The shared subscription is local priority, indicating that it has been sent
			if n == a.GetLocalName() {
				return ns
			}
		}
		// Share subscription Select a node at random
		n := tmpNs[rand.Intn(len(tmpNs))]
		ns = []string{n}
		return ns
	}

	// Not shared subscriptions are returned as is
	ns = tmpNs
	return
}

func OnJoinLog(nodeId, addr, prompt string, err error) {
	if err != nil {
		log.Error(prompt, "error", err, "node", nodeId, "addr", addr)
	} else {
		log.Info(prompt, "node", nodeId, "addr", addr)
	}
}

func OnApplyLog(leaderId, nodeId string, tp byte, filter []byte, prompt string, err error) {
	if err != nil {
		log.Error(prompt, "error", err, "leader", leaderId, "from", nodeId, "type", tp, "filter", filter)
	} else {
		log.Info(prompt, "leader", leaderId, "from", nodeId, "type", tp, "filter", filter)
	}
}

func OnPublishPacketLog(direction byte, nodeId, cid, topic string, pid uint16) {
	if direction == DirectionInbound {
		log.Info("publish message", "d", "inbound", "from", nodeId, "cid", cid, "pid", pid, "topic", topic)
	} else {
		log.Info("publish message", "d", "outbound", "to", nodeId, "cid", cid, "pid", pid, "topic", topic)
	}
}

func OnConnectPacketLog(direction byte, node, clientId string) {
	if direction == DirectionInbound {
		log.Info("connection notification", "d", "inbound", "from", node, "cid", clientId)
	} else {
		log.Info("connection notification", "d", "outbound", "to", node, "cid", clientId)
	}
}

func (a *Agent) Join(nodeName, addr string) error {
	var existingNode string
	if nodeName != "" {
		existingNode += nodeName
		existingNode += "/"
	}
	existingNode += addr
	_, err := a.membership.Join([]string{existingNode})
	return err
}

func (a *Agent) Leave() error {
	return a.membership.Leave()
}

func (a *Agent) AddRaftPeer(id, addr string) {
	if err := a.raftPeer.Join(id, addr); err != nil {
		log.Error("add raft peer failed", "error", err, "nid", id, "addr", addr)
		return
	}
	log.Info("add peer", "nid", id, "addr", addr)
}

func (a *Agent) RemoveRaftPeer(id string) {
	if err := a.raftPeer.Leave(id); err != nil {
		log.Error("remove raft peer failed", "error", err, "nid", id)
		return
	}
	log.Info("remove peer", "nid", id)
}

func (a *Agent) GetValue(key string) []string {
	log.Info("get value", "key", key)
	return a.raftPeer.Lookup(key)
}

// banSweepInterval is how often the leader purges expired temporary bans
// from the replicated ban book. Enforcement never depends on the sweep:
// reads treat expired generations as inactive (lazy expiry).
const banSweepInterval = 30 * time.Second

// BanClient proposes a cluster-wide ban for clientID. A ttl <= 0 means a
// permanent ban. The ban generation becomes durable through raft: it
// survives node restarts, is replicated to nodes that were briefly offline
// when they catch up, and every node disconnects the client's live
// connection on apply. Proposing is idempotent: retries after timeouts
// never produce a weaker result than the newest committed generation.
func (a *Agent) BanClient(clientID, reason string, ttl time.Duration) (mqtt.BanPolicy, error) {
	now := time.Now()
	createdAt := now.UnixNano()
	expiresAt := int64(raft.BanExpiresNever)
	if ttl > 0 {
		expiresAt = now.Add(ttl).UnixNano()
	}

	ban := raft.Ban{ClientID: clientID, Reason: reason, CreatedAt: createdAt, ExpiresAt: expiresAt}
	payload, err := json.Marshal(ban)
	if err != nil {
		return mqtt.BanPolicy{}, err
	}
	msg := &message.Message{
		Type:     message.BanAdd,
		NodeID:   a.GetLocalName(),
		ClientID: clientID,
		Payload:  payload,
	}

	if a.raftPeer.IsApplyRight() {
		err = a.raftPeer.Propose(msg)
		OnApplyLog(a.GetLocalName(), msg.NodeID, msg.Type, []byte(msg.ClientID), "raft ban apply", err)
	} else {
		// follower: hand over to the leader (or broadcast when no leader is
		// known); delivery is best-effort exactly like subscription proposes.
		a.raftPropose(msg)
	}
	if err != nil {
		return mqtt.BanPolicy{}, err
	}

	policy := mqtt.BanPolicy{
		ClientID:  clientID,
		Reason:    reason,
		Permanent: expiresAt == raft.BanExpiresNever,
		CreatedAt: createdAt / int64(time.Second),
		ExpiresAt: expiresAt / int64(time.Second),
		Remaining: banTTLSeconds(expiresAt, now.UnixNano()),
	}
	return policy, nil
}

// UnbanClient proposes removal of the ban generation for clientID. The
// removal is fenced by the request time so that a delayed/stale unban never
// deletes a newer ban generation.
func (a *Agent) UnbanClient(clientID string) error {
	rm := raft.BanRemoval{ClientID: clientID, At: time.Now().UnixNano()}
	payload, err := json.Marshal(rm)
	if err != nil {
		return err
	}
	msg := &message.Message{
		Type:     message.BanDel,
		NodeID:   a.GetLocalName(),
		ClientID: clientID,
		Payload:  payload,
	}
	if a.raftPeer.IsApplyRight() {
		err = a.raftPeer.Propose(msg)
		OnApplyLog(a.GetLocalName(), msg.NodeID, msg.Type, []byte(msg.ClientID), "raft unban apply", err)
	} else {
		a.raftPropose(msg)
	}
	return err
}

// BanList returns the cluster-wide ban policies as mirrored on this node.
// The mirror is the raft state applied so far, so after catch-up it is
// identical on every node.
func (a *Agent) BanList() []mqtt.BanPolicy {
	return a.mqttServer.BanList()
}

// banSweeper periodically proposes removal of expired temporary ban
// generations. Only the leader proposes, which keeps duplicate log entries
// out; a leadership change simply moves the job. Cleanup entries identify
// the exact generation they observed, so a re-ban racing the cleanup is
// never deleted.
func (a *Agent) banSweeper() {
	ticker := time.NewTicker(banSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if a.raftPeer == nil || !a.raftPeer.IsApplyRight() {
				continue
			}
			now := time.Now().UnixNano()
			for _, ban := range a.raftPeer.ListBans() {
				if ban.ExpiresAt == raft.BanExpiresNever || now < ban.ExpiresAt {
					continue
				}
				payload, err := json.Marshal(raft.BanRemoval{
					ClientID:  ban.ClientID,
					CreatedAt: ban.CreatedAt,
					ExpiresAt: ban.ExpiresAt,
				})
				if err != nil {
					continue
				}
				a.raftPropose(&message.Message{
					Type:     message.BanDel,
					NodeID:   a.GetLocalName(),
					ClientID: ban.ClientID,
					Payload:  payload,
				})
				log.Info("raft ban expiry cleanup proposed", "cid", ban.ClientID)
			}
		case <-a.ctx.Done():
			return
		}
	}
}

func banTTLSeconds(expiresAt, now int64) int64 {
	if expiresAt == raft.BanExpiresNever {
		return -1
	}
	if expiresAt <= now {
		return 0
	}
	return (expiresAt - now) / int64(time.Second)
}
