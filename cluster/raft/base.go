// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: 2022 wind
// SPDX-FileContributor: wind (573966@qq.com)

package raft

import (
	"encoding/gob"
	"io"
	"sync"

	"github.com/wind-c/comqtt/v2/cluster/message"
	"github.com/wind-c/comqtt/v2/cluster/utils"
)

type IPeer interface {
	Join(nodeID, addr string) error
	Leave(nodeID string) error
	Propose(msg *message.Message) error
	Lookup(key string) []string
	ListBans() []Ban
	IsApplyRight() bool
	GetLeader() (addr, id string)
	GenPeersFile(file string) error
	Stop()
}

type data map[string][]string

type KV struct {
	data data
	sync.RWMutex
}

func NewKV() *KV {
	return &KV{
		data: make(map[string][]string),
	}
}

func (k *KV) GetAll() *data {
	k.RLock()
	defer k.RUnlock()
	cp := make(data, len(k.data))
	for k, v := range k.data {
		cp[k] = append([]string(nil), v...)
	}
	return &cp
}

// Restore replaces the store contents with gob-encoded data read from r.
// A raft snapshot holds the complete state up to its index, so existing
// entries are discarded rather than merged.
func (k *KV) Restore(r io.Reader) error {
	var d data
	if err := gob.NewDecoder(r).Decode(&d); err != nil {
		return err
	}
	if d == nil {
		d = make(data)
	}
	k.Lock()
	defer k.Unlock()
	k.data = d
	return nil
}

// RestoreSnapshot decodes a snapshot stream that holds the subscription map
// first and the ban-policy map second (the bans object may be absent in
// snapshots written by older versions). A single gob.Decoder is used for
// both objects: gob buffers its reads, so a second decoder on the same
// reader would only see EOF for wrapped readers that hide the underlying
// Reader's ReadByte method.
func (k *KV) RestoreSnapshot(r io.Reader, bk *BanBook) error {
	dec := gob.NewDecoder(r)

	var d data
	if err := dec.Decode(&d); err != nil {
		return err
	}
	k.Lock()
	if d == nil {
		d = make(data)
	}
	k.data = d
	k.Unlock()

	var bans map[string]Ban
	if err := dec.Decode(&bans); err != nil && err != io.EOF {
		return err
	}
	bk.Restore(bans)
	return nil
}

func (k *KV) Get(key string) []string {
	k.RLock()
	defer k.RUnlock()
	vs := k.data[key]
	return vs
}

// Add return true if key is set for the first time
func (k *KV) Add(key, value string) (new bool) {
	k.Lock()
	defer k.Unlock()
	if vs, ok := k.data[key]; ok {
		if utils.Contains(vs, value) {
			return
		}
		k.data[key] = append(vs, value)
	} else {
		k.data[key] = []string{value}
		new = true
	}
	return
}

// Del return true if the array corresponding to key is deleted
// If the value is "", the key-values pair is deleted
func (k *KV) Del(key, value string) (empty bool) {
	k.Lock()
	defer k.Unlock()
	if vs, ok := k.data[key]; ok {
		if utils.Contains(vs, value) {
			for i, item := range vs {
				if item == value {
					k.data[key] = append(vs[:i], vs[i+1:]...)
				}
			}
		}

		if value == "" || len(k.data[key]) == 0 {
			delete(k.data, key)
			empty = true
		}
	}
	return
}

// DelByValue delete the specified value from the key-values array
// and delete the key-value pair if the key-values array is empty
func (k *KV) DelByValue(value string) int {
	k.Lock()
	defer k.Unlock()
	c := 0
	if value == "" {
		return c
	}

	for f, vs := range k.data {
		for i, v := range vs {
			if v == value {
				k.data[f] = append(vs[:i], vs[i+1:]...)
				c++
			}
		}
		if len(k.data[f]) == 0 {
			delete(k.data, f)
		}
	}
	return c
}

// BanExpiresNever is the ExpiresAt value of a permanent ban.
const BanExpiresNever int64 = 0

// Ban is a client ban policy replicated through raft. Timestamps are unix
// nanoseconds so that concurrent actions on different nodes keep a total
// ordering even within the same wall-clock second.
type Ban struct {
	ClientID  string `json:"cid"`
	Reason    string `json:"r,omitempty"`
	CreatedAt int64  `json:"ct"` // request time of the ban, acts as fencing token
	ExpiresAt int64  `json:"et"` // unix nano; BanExpiresNever (0) means permanent
}

// Permanent reports whether the ban never expires.
func (b Ban) Permanent() bool {
	return b.ExpiresAt == BanExpiresNever
}

// Expired reports whether the ban has expired at the given unix nano time.
func (b Ban) Expired(now int64) bool {
	return b.ExpiresAt != BanExpiresNever && now >= b.ExpiresAt
}

// BanRemoval is the payload of a BanDel raft entry. Two mutually exclusive
// modes share one message type:
//   - explicit unban: At is set, the ban generation is removed when its
//     CreatedAt <= At, so a ban created after the unban request is preserved;
//   - expiry cleanup: CreatedAt and ExpiresAt identify the exact generation
//     observed by the sweeper, so a re-ban (different generation) is never
//     deleted by a stale cleanup entry.
type BanRemoval struct {
	ClientID  string `json:"cid"`
	At        int64  `json:"at,omitempty"` // explicit unban request time (unix nano)
	CreatedAt int64  `json:"ct,omitempty"` // expiry cleanup: generation creation time
	ExpiresAt int64  `json:"et,omitempty"` // expiry cleanup: generation expiry time
}

// BanBook is the replicated client-ban state machine. All mutating methods
// are designed to be applied deterministically in raft log order on every
// node, so retries, reordered proposals and expiry-cleanup/new-ban races
// converge to the same result everywhere.
type BanBook struct {
	sync.RWMutex
	bans map[string]Ban
}

func NewBanBook() *BanBook {
	return &BanBook{bans: make(map[string]Ban)}
}

// Get returns the ban generation currently stored for the client.
func (bk *BanBook) Get(clientID string) (Ban, bool) {
	bk.RLock()
	defer bk.RUnlock()
	b, ok := bk.bans[clientID]
	return b, ok
}

// All returns a copy of all stored ban generations (including expired ones
// that have not been physically purged yet).
func (bk *BanBook) All() []Ban {
	bk.RLock()
	defer bk.RUnlock()
	out := make([]Ban, 0, len(bk.bans))
	for _, b := range bk.bans {
		out = append(out, b)
	}
	return out
}

// Put applies a ban generation. Stale proposals (older CreatedAt than the
// stored generation) are ignored; equal CreatedAt is an idempotent retry.
// Returns true when the stored generation changed or was confirmed.
func (bk *BanBook) Put(b Ban) bool {
	bk.Lock()
	defer bk.Unlock()
	if cur, ok := bk.bans[b.ClientID]; ok && cur.CreatedAt > b.CreatedAt {
		return false
	}
	bk.bans[b.ClientID] = b
	return true
}

// Remove applies an explicit unban: the stored generation is deleted only
// when its CreatedAt <= notAfter (the unban request time), so a ban created
// after the unban was requested survives a delayed/replayed unban entry.
func (bk *BanBook) Remove(clientID string, notAfter int64) bool {
	bk.Lock()
	defer bk.Unlock()
	cur, ok := bk.bans[clientID]
	if !ok || cur.CreatedAt > notAfter {
		return false
	}
	delete(bk.bans, clientID)
	return true
}

// RemoveGeneration deletes a ban only when the stored generation matches the
// exact (CreatedAt, ExpiresAt) pair observed by the expiry sweeper. A newer
// re-ban therefore never gets purged by a stale cleanup entry.
func (bk *BanBook) RemoveGeneration(clientID string, createdAt, expiresAt int64) bool {
	bk.Lock()
	defer bk.Unlock()
	cur, ok := bk.bans[clientID]
	if !ok || cur.CreatedAt != createdAt || cur.ExpiresAt != expiresAt {
		return false
	}
	delete(bk.bans, clientID)
	return true
}

// Snapshot returns a deep copy of the stored bans for raft snapshots.
func (bk *BanBook) Snapshot() map[string]Ban {
	bk.RLock()
	defer bk.RUnlock()
	cp := make(map[string]Ban, len(bk.bans))
	for k, v := range bk.bans {
		cp[k] = v
	}
	return cp
}

// Restore replaces the whole book with the snapshot state. A raft snapshot
// holds the complete state up to its index, so existing entries are
// discarded rather than merged.
func (bk *BanBook) Restore(bans map[string]Ban) {
	bk.Lock()
	defer bk.Unlock()
	if bans == nil {
		bans = make(map[string]Ban)
	}
	bk.bans = bans
}
