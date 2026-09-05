// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 wind
// SPDX-FileContributor: wind (573966@qq.com)

package etcd

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/wind-c/comqtt/v2/cluster/log"
	"github.com/wind-c/comqtt/v2/cluster/message"
	base "github.com/wind-c/comqtt/v2/cluster/raft"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
	"strings"
)

// KVStore is a key-value store backed by raft
type KVStore struct {
	*base.KV
	bans        *base.BanBook
	snapshotter *snap.Snapshotter
	commitC     <-chan *commit
	errorC      <-chan error
	notifyCh    chan<- *message.Message
}

func newKVStore(snapshotter *snap.Snapshotter, commitC <-chan *commit, errorC <-chan error, notifyCh chan<- *message.Message) *KVStore {
	s := &KVStore{
		KV:          base.NewKV(),
		bans:        base.NewBanBook(),
		snapshotter: snapshotter,
		commitC:     commitC,
		errorC:      errorC,
		notifyCh:    notifyCh,
	}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Fatal("[store] load snapshot", "error", err)
	}
	if snapshot != nil {
		log.Info("[store] loading snapshot at term and index", "term", snapshot.Metadata.Term, "index", snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Fatal("[store] recover snapshot", "error", err)
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits()
	return s
}

func (s *KVStore) Lookup(key string) []string {
	return s.Get(key)
}

func (s *KVStore) DelByNode(node string) int {
	return s.DelByValue(node)
}

// ListBans returns all replicated ban generations, including expired ones
// not yet purged. Callers treat expired entries as inactive.
func (s *KVStore) ListBans() []base.Ban {
	return s.bans.All()
}

func (s *KVStore) GetErrorC(key, value string) <-chan error {
	return s.errorC
}

func (s *KVStore) readCommits() {
	for commit := range s.commitC {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Fatal("[store] load snapshot", "error", err)
			}
			if snapshot != nil {
				log.Info("[store] loading snapshot at term and index", "term", snapshot.Metadata.Term, "index", snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Fatal("[store] recover snapshot", "error", err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var msg message.Message
			if err := msg.MsgpackLoad(data); err != nil {
				continue
			}
			filter := string(msg.Payload)
			deliverable := false
			switch {
			case msg.Type == packets.Subscribe:
				deliverable = s.Add(filter, msg.NodeID)
				log.Info("raft apply", "from", msg.NodeID, "filter", filter, "type", msg.Type)
			case msg.Type == packets.Unsubscribe:
				deliverable = s.Del(filter, msg.NodeID)
				log.Info("raft apply", "from", msg.NodeID, "filter", filter, "type", msg.Type)
			case msg.Type == message.BanAdd:
				var ban base.Ban
				if err := json.Unmarshal(msg.Payload, &ban); err != nil {
					log.Error("[store] unmarshal ban", "error", err)
					continue
				}
				s.bans.Put(ban)
				log.Info("raft apply ban", "from", msg.NodeID, "cid", ban.ClientID, "expires", ban.ExpiresAt)
				// ban notifications must reach every node (including the
				// proposer) so the local mirror is updated and the banned
				// client's live connection is torn down.
				deliverable = true
			case msg.Type == message.BanDel:
				var rm base.BanRemoval
				if err := json.Unmarshal(msg.Payload, &rm); err != nil {
					log.Error("[store] unmarshal ban removal", "error", err)
					continue
				}
				if rm.CreatedAt > 0 && rm.ExpiresAt > 0 {
					s.bans.RemoveGeneration(rm.ClientID, rm.CreatedAt, rm.ExpiresAt)
				} else {
					s.bans.Remove(rm.ClientID, rm.At)
				}
				log.Info("raft apply unban", "from", msg.NodeID, "cid", rm.ClientID)
				deliverable = true
			default:
				continue
			}
			if s.notifyCh != nil && deliverable {
				s.notifyCh <- &msg
			}
		}
		close(commit.applyDoneC)
	}
	if err, ok := <-s.errorC; ok {
		log.Fatal("[store] read commit", "error", err)
	}
}

func (s *KVStore) getSnapshot() ([]byte, error) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(s.GetAll()); err != nil {
		return nil, err
	}
	// bans are appended as a second gob object only when present, so that
	// ban-free snapshots keep the exact historical format; older versions
	// reading them (and readers hitting EOF) treat missing bans as empty.
	if bans := s.bans.Snapshot(); len(bans) > 0 {
		if err := gob.NewEncoder(&buffer).Encode(bans); err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func (s *KVStore) loadSnapshot() (*raftpb.Snapshot, error) {
	if snapshot, err := s.snapshotter.Load(); err != nil {
		if err == snap.ErrNoSnapshot {
			return nil, nil
		} else {
			return nil, err
		}
	} else {
		return snapshot, nil
	}
}

func (s *KVStore) recoverFromSnapshot(snapshot []byte) error {
	reader := bytes.NewReader(snapshot)
	// subscriptions map first, ban-policy map second (absent in old snapshots);
	// a single decoder must read both because gob buffers its reads.
	if err := s.KV.RestoreSnapshot(reader, s.bans); err != nil {
		return err
	}
	s.notifyReplay()
	return nil
}

func (s *KVStore) notifyReplay() {
	for filter, ns := range *s.GetAll() {
		msg := message.Message{
			Type:    packets.Subscribe,
			NodeID:  strings.Join(ns, ","),
			Payload: []byte(filter),
		}
		s.notifyCh <- &msg
		log.Info("raft replay", "from", msg.NodeID, "filter", filter, "type", msg.Type)
	}
	for _, ban := range s.bans.All() {
		payload, err := json.Marshal(ban)
		if err != nil {
			continue
		}
		msg := message.Message{
			Type:     message.BanAdd,
			ClientID: ban.ClientID,
			Payload:  payload,
		}
		s.notifyCh <- &msg
		log.Info("raft replay ban", "cid", ban.ClientID, "expires", ban.ExpiresAt)
	}
}
