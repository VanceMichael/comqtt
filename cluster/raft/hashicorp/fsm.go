// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 wind
// SPDX-FileContributor: wind (573966@qq.com)

package hashicorp

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io"
	"strings"

	"github.com/hashicorp/raft"
	"github.com/wind-c/comqtt/v2/cluster/log"
	"github.com/wind-c/comqtt/v2/cluster/message"
	base "github.com/wind-c/comqtt/v2/cluster/raft"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

type Fsm struct {
	*base.KV
	bans     *base.BanBook
	notifyCh chan<- *message.Message
}

func NewFsm(notifyCh chan<- *message.Message) *Fsm {
	fsm := &Fsm{
		KV:       base.NewKV(),
		bans:     base.NewBanBook(),
		notifyCh: notifyCh,
	}
	return fsm
}

func (f *Fsm) Apply(l *raft.Log) interface{} {
	var msg message.Message
	if err := msg.MsgpackLoad(l.Data); err != nil {
		return nil
	}
	filter := string(msg.Payload)
	deliverable := false
	switch {
	case msg.Type == packets.Subscribe:
		deliverable = f.Add(filter, msg.NodeID)
		log.Info("raft apply", "from", msg.NodeID, "filter", filter, "type", msg.Type)
	case msg.Type == packets.Unsubscribe:
		deliverable = f.Del(filter, msg.NodeID)
		log.Info("raft apply", "from", msg.NodeID, "filter", filter, "type", msg.Type)
	case msg.Type == message.BanAdd:
		var ban base.Ban
		if err := json.Unmarshal(msg.Payload, &ban); err != nil {
			log.Error("raft apply ban unmarshal", "error", err)
			return nil
		}
		f.bans.Put(ban)
		log.Info("raft apply ban", "from", msg.NodeID, "cid", ban.ClientID, "expires", ban.ExpiresAt)
		// ban notifications must reach every node (including the proposer)
		// so the local mirror is updated and the live connection is torn down.
		deliverable = true
	case msg.Type == message.BanDel:
		var rm base.BanRemoval
		if err := json.Unmarshal(msg.Payload, &rm); err != nil {
			log.Error("raft apply unban unmarshal", "error", err)
			return nil
		}
		if rm.CreatedAt > 0 && rm.ExpiresAt > 0 {
			f.bans.RemoveGeneration(rm.ClientID, rm.CreatedAt, rm.ExpiresAt)
		} else {
			f.bans.Remove(rm.ClientID, rm.At)
		}
		log.Info("raft apply unban", "from", msg.NodeID, "cid", rm.ClientID)
		deliverable = true
	default:
		return nil
	}
	if f.notifyCh != nil && deliverable {
		select {
		case f.notifyCh <- &msg:
		default: // channel is full, drop notification but do not block Raft
			log.Warn("notify channel full, dropping notification")
		}
	}

	return nil
}

func (f *Fsm) Lookup(key string) []string {
	return f.Get(key)
}

// ListBans returns all replicated ban generations, including expired ones
// not yet purged. Callers treat expired entries as inactive.
func (f *Fsm) ListBans() []base.Ban {
	return f.bans.All()
}

func (f *Fsm) DelByNode(node string) int {
	return f.DelByValue(node)
}

func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	return f, nil
}

func (f *Fsm) Restore(ir io.ReadCloser) error {
	// subscriptions map first, ban-policy map second (absent in old snapshots);
	// a single decoder must read both because gob buffers its reads.
	if err := f.KV.RestoreSnapshot(ir, f.bans); err != nil {
		return err
	}
	f.notifyReplay()
	return nil
}

func (f *Fsm) notifyReplay() {
	for filter, ns := range *f.GetAll() {
		msg := message.Message{
			Type:    packets.Subscribe,
			NodeID:  strings.Join(ns, ","),
			Payload: []byte(filter),
		}
		f.notifyCh <- &msg
		log.Info("raft replay", "from", msg.NodeID, "filter", filter, "type", msg.Type)
	}
	for _, ban := range f.bans.All() {
		payload, err := json.Marshal(ban)
		if err != nil {
			continue
		}
		msg := message.Message{
			Type:     message.BanAdd,
			ClientID: ban.ClientID,
			Payload:  payload,
		}
		f.notifyCh <- &msg
		log.Info("raft replay ban", "cid", ban.ClientID, "expires", ban.ExpiresAt)
	}
}

func (f *Fsm) Persist(sink raft.SnapshotSink) error {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(f.GetAll()); err != nil {
		return err
	}
	// bans are appended as a second gob object only when present, so that
	// ban-free snapshots keep the exact historical format.
	if bans := f.bans.Snapshot(); len(bans) > 0 {
		if err := gob.NewEncoder(&buffer).Encode(bans); err != nil {
			return err
		}
	}
	if _, err := sink.Write(buffer.Bytes()); err != nil {
		return err
	}
	return sink.Close()
}

func (f *Fsm) Release() {}
