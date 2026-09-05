// SPDX-License-Identifier: MIT
// SPDX-FileContributor: wind (573966@qq.com)

package raft

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestBanBook_PutNewerWins(t *testing.T) {
	bk := NewBanBook()
	base := time.Now().UnixNano()

	require.True(t, bk.Put(Ban{ClientID: "c1", CreatedAt: base, ExpiresAt: base + 1000}))

	// older generation (delayed/reordered proposal) must not overwrite
	require.False(t, bk.Put(Ban{ClientID: "c1", CreatedAt: base - 10, ExpiresAt: base + 500}))
	got, ok := bk.Get("c1")
	require.True(t, ok)
	require.Equal(t, base+1000, got.ExpiresAt)

	// same CreatedAt is an idempotent retry
	require.True(t, bk.Put(Ban{ClientID: "c1", CreatedAt: base, ExpiresAt: base + 1000, Reason: "again"}))
	got, _ = bk.Get("c1")
	require.Equal(t, "again", got.Reason)

	// newer generation (re-ban) overwrites
	require.True(t, bk.Put(Ban{ClientID: "c1", CreatedAt: base + 20, ExpiresAt: BanExpiresNever}))
	got, _ = bk.Get("c1")
	require.True(t, got.Permanent())
}

func TestBanBook_RemoveFencedByTime(t *testing.T) {
	bk := NewBanBook()
	ct := int64(1000)
	bk.Put(Ban{ClientID: "c1", CreatedAt: ct, ExpiresAt: 5000})

	// unban requested before the ban existed (stale/delayed unban) must not remove
	require.False(t, bk.Remove("c1", ct-1))
	_, ok := bk.Get("c1")
	require.True(t, ok)

	// unban requested after the ban removes it
	require.True(t, bk.Remove("c1", ct+100))
	_, ok = bk.Get("c1")
	require.False(t, ok)
	require.False(t, bk.Remove("c1", ct+200))
}

func TestBanBook_RemoveDoesNotTouchNewerReban(t *testing.T) {
	bk := NewBanBook()
	// ban at t=1000, unban requested at t=2000, then re-ban at t=3000
	bk.Put(Ban{ClientID: "c1", CreatedAt: 3000, ExpiresAt: 9000})
	// a delayed unban from t=2000 arrives after the re-ban
	require.False(t, bk.Remove("c1", 2000))
	got, ok := bk.Get("c1")
	require.True(t, ok)
	require.Equal(t, int64(3000), got.CreatedAt)
}

func TestBanBook_RemoveGeneration(t *testing.T) {
	bk := NewBanBook()
	bk.Put(Ban{ClientID: "c1", CreatedAt: 1000, ExpiresAt: 2000})

	// cleanup for a different generation (newer re-ban) must not delete
	bk.Put(Ban{ClientID: "c1", CreatedAt: 3000, ExpiresAt: 4000})
	require.False(t, bk.RemoveGeneration("c1", 1000, 2000))
	got, ok := bk.Get("c1")
	require.True(t, ok)
	require.Equal(t, int64(3000), got.CreatedAt)

	// exact generation match deletes
	require.True(t, bk.RemoveGeneration("c1", 3000, 4000))
	_, ok = bk.Get("c1")
	require.False(t, ok)
}

func TestBanBook_ExpirySweepScenario(t *testing.T) {
	bk := NewBanBook()
	now := time.Now().UnixNano()

	// expired temporary ban
	bk.Put(Ban{ClientID: "expired", CreatedAt: now - 2000, ExpiresAt: now - 1000})
	// active temporary ban
	bk.Put(Ban{ClientID: "active", CreatedAt: now, ExpiresAt: now + 1000})
	// permanent ban
	bk.Put(Ban{ClientID: "forever", CreatedAt: now, ExpiresAt: BanExpiresNever})

	all := bk.All()
	require.Len(t, all, 3)

	// the leader's sweeper would remove only the expired generation
	require.True(t, bk.RemoveGeneration("expired", now-2000, now-1000))
	_, ok := bk.Get("expired")
	require.False(t, ok)

	// permanent and active generations survive
	_, ok = bk.Get("forever")
	require.True(t, ok)
	got, ok := bk.Get("active")
	require.True(t, ok)
	require.False(t, got.Expired(now))
	require.False(t, got.Permanent())

	forever, _ := bk.Get("forever")
	require.True(t, forever.Permanent())
	require.False(t, forever.Expired(now))
}

func TestBanBook_SnapshotRestore(t *testing.T) {
	src := NewBanBook()
	src.Put(Ban{ClientID: "c1", Reason: "r1", CreatedAt: 1, ExpiresAt: 2})
	src.Put(Ban{ClientID: "c2", CreatedAt: 3, ExpiresAt: BanExpiresNever})

	dst := NewBanBook()
	dst.Put(Ban{ClientID: "stale", CreatedAt: 9, ExpiresAt: 10})
	dst.Restore(src.Snapshot())

	_, ok := dst.Get("stale")
	require.False(t, ok)
	b, ok := dst.Get("c1")
	require.True(t, ok)
	require.Equal(t, "r1", b.Reason)
	b2, ok := dst.Get("c2")
	require.True(t, ok)
	require.True(t, b2.Permanent())
}
