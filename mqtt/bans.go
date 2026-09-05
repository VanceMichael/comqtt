// SPDX-License-Identifier: MIT
// SPDX-FileContributor: wind (573966@qq.com)

package mqtt

import (
	"sort"
	"sync"
	"time"
)

// BanPolicy is the external, JSON-serializable view of a client ban.
// Timestamps are unix seconds for the management API/UI; the internal book
// keeps nanosecond precision so concurrent actions within the same second
// stay ordered.
type BanPolicy struct {
	ClientID  string `json:"client_id"`
	Reason    string `json:"reason,omitempty"`
	Permanent bool   `json:"permanent"`
	CreatedAt int64  `json:"created_at"`        // unix seconds
	ExpiresAt int64  `json:"expires_at"`        // unix seconds, 0 when permanent
	Remaining int64  `json:"remaining_seconds"` // -1 permanent; otherwise seconds left
}

// banEntry is one ban generation. Timestamps are unix nanoseconds;
// expiresAt == 0 means the ban is permanent.
type banEntry struct {
	reason    string
	createdAt int64
	expiresAt int64
}

// banBook holds client ban generations. It is the enforcement point used by
// every deployment: standalone mode writes it directly through the REST
// handlers, while cluster mode mirrors raft-committed generations into it
// via ApplyBan/ApplyBanRemoval/RemoveBanGeneration. All writes are
// compare-and-swap style so retries, reordered proposals and expiry-cleanup
// races converge to the same result.
type banBook struct {
	mu   sync.RWMutex
	bans map[string]banEntry
}

func newBanBook() *banBook {
	return &banBook{bans: make(map[string]banEntry)}
}

// put stores a ban generation unless a newer one already exists. Equal
// createdAt is treated as an idempotent retry.
func (bk *banBook) put(clientID, reason string, createdAt, expiresAt int64) bool {
	bk.mu.Lock()
	defer bk.mu.Unlock()
	if cur, ok := bk.bans[clientID]; ok && cur.createdAt > createdAt {
		return false
	}
	bk.bans[clientID] = banEntry{reason: reason, createdAt: createdAt, expiresAt: expiresAt}
	return true
}

// remove deletes the generation for clientID unless it was created after
// notAfter (an explicit unban request time, unix nano).
func (bk *banBook) remove(clientID string, notAfter int64) bool {
	bk.mu.Lock()
	defer bk.mu.Unlock()
	cur, ok := bk.bans[clientID]
	if !ok || cur.createdAt > notAfter {
		return false
	}
	delete(bk.bans, clientID)
	return true
}

// removeGeneration deletes a ban only when the stored generation matches the
// exact (createdAt, expiresAt) pair, so a stale expiry cleanup never purges a
// newer re-ban.
func (bk *banBook) removeGeneration(clientID string, createdAt, expiresAt int64) bool {
	bk.mu.Lock()
	defer bk.mu.Unlock()
	cur, ok := bk.bans[clientID]
	if !ok || cur.createdAt != createdAt || cur.expiresAt != expiresAt {
		return false
	}
	delete(bk.bans, clientID)
	return true
}

// banned reports whether the client is currently banned. Expired temporary
// bans are treated as inactive (lazy expiry).
func (bk *banBook) banned(clientID string, now int64) bool {
	bk.mu.RLock()
	defer bk.mu.RUnlock()
	cur, ok := bk.bans[clientID]
	return ok && (cur.expiresAt == 0 || now < cur.expiresAt)
}

// list returns the active (non-expired) ban generations, sorted by client id
// for stable API output.
func (bk *banBook) list(now int64) []BanPolicy {
	bk.mu.RLock()
	defer bk.mu.RUnlock()
	out := make([]BanPolicy, 0, len(bk.bans))
	for cid, e := range bk.bans {
		if e.expiresAt != 0 && now >= e.expiresAt {
			continue
		}
		p := BanPolicy{
			ClientID:  cid,
			Reason:    e.reason,
			Permanent: e.expiresAt == 0,
			CreatedAt: e.createdAt / int64(time.Second),
			ExpiresAt: e.expiresAt / int64(time.Second),
			Remaining: -1,
		}
		if e.expiresAt != 0 {
			p.Remaining = (e.expiresAt - now) / int64(time.Second)
			if p.Remaining < 0 {
				p.Remaining = 0
			}
		}
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ClientID < out[j].ClientID })
	return out
}

// sweep physically removes expired temporary generations. Permanent bans and
// future-dated generations are left untouched, so a concurrent re-ban can
// never be purged.
func (bk *banBook) sweep(now int64) int {
	bk.mu.Lock()
	defer bk.mu.Unlock()
	c := 0
	for cid, e := range bk.bans {
		if e.expiresAt != 0 && now >= e.expiresAt {
			delete(bk.bans, cid)
			c++
		}
	}
	return c
}

// BanClient adds a local ban for the client and returns its policy. A ttl <= 0
// means permanent. Used directly in standalone mode and for node-local REST
// calls; cluster mode applies raft-committed generations via ApplyBan.
func (s *Server) BanClient(clientID, reason string, ttl time.Duration) BanPolicy {
	now := time.Now()
	createdAt := now.UnixNano()
	expiresAt := int64(0)
	if ttl > 0 {
		expiresAt = now.Add(ttl).UnixNano()
	}
	s.bans.put(clientID, reason, createdAt, expiresAt)
	return banPolicy(clientID, reason, createdAt, expiresAt, now.UnixNano())
}

// UnbanClient explicitly removes the ban generation for the client.
// Returns true when a generation was removed.
func (s *Server) UnbanClient(clientID string) bool {
	return s.bans.remove(clientID, time.Now().UnixNano())
}

// IsBanned reports whether the client currently has an active ban.
func (s *Server) IsBanned(clientID string) bool {
	return s.bans.banned(clientID, time.Now().UnixNano())
}

// BanList returns all active ban policies with remaining time.
func (s *Server) BanList() []BanPolicy {
	return s.bans.list(time.Now().UnixNano())
}

// ApplyBan mirrors a raft-committed ban generation into the local book.
func (s *Server) ApplyBan(clientID, reason string, createdAt, expiresAt int64) bool {
	return s.bans.put(clientID, reason, createdAt, expiresAt)
}

// ApplyBanRemoval mirrors a raft-committed explicit unban. notAfter is the
// unban request time (unix nano); generations created later are preserved.
func (s *Server) ApplyBanRemoval(clientID string, notAfter int64) bool {
	return s.bans.remove(clientID, notAfter)
}

// RemoveBanGeneration mirrors a raft-committed expiry cleanup; it only removes
// the exact generation identified by (createdAt, expiresAt).
func (s *Server) RemoveBanGeneration(clientID string, createdAt, expiresAt int64) bool {
	return s.bans.removeGeneration(clientID, createdAt, expiresAt)
}

// sweepBans physically purges expired temporary ban generations. In cluster
// mode the raft log remains authoritative: a purged mirror entry is either
// re-confirmed by the leader's replicated cleanup or re-added by a newer ban,
// and enforcement while expired is identical to no entry.
func (s *Server) sweepBans(now int64) int {
	return s.bans.sweep(now)
}

func banPolicy(clientID, reason string, createdAt, expiresAt, now int64) BanPolicy {
	p := BanPolicy{
		ClientID:  clientID,
		Reason:    reason,
		Permanent: expiresAt == 0,
		CreatedAt: createdAt / int64(time.Second),
		ExpiresAt: expiresAt / int64(time.Second),
		Remaining: -1,
	}
	if expiresAt != 0 {
		p.Remaining = (expiresAt - now) / int64(time.Second)
		if p.Remaining < 0 {
			p.Remaining = 0
		}
	}
	return p
}
