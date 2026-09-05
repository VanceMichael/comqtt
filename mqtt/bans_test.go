// SPDX-License-Identifier: MIT
// SPDX-FileContributor: wind (573966@qq.com)

package mqtt

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

func TestServerBanClientTemporary(t *testing.T) {
	s := newServer()
	defer s.Close()

	p := s.BanClient("temp-1", "abuse", time.Minute)
	require.Equal(t, "temp-1", p.ClientID)
	require.False(t, p.Permanent)
	require.Equal(t, int64(60), p.Remaining)
	require.True(t, s.IsBanned("temp-1"))
	require.False(t, s.IsBanned("other"))

	list := s.BanList()
	require.Len(t, list, 1)
	require.Equal(t, "abuse", list[0].Reason)

	require.True(t, s.UnbanClient("temp-1"))
	require.False(t, s.IsBanned("temp-1"))
	require.Len(t, s.BanList(), 0)
	// unban is idempotent
	require.False(t, s.UnbanClient("temp-1"))
}

func TestServerBanClientPermanent(t *testing.T) {
	s := newServer()
	defer s.Close()

	p := s.BanClient("forever", "", 0)
	require.True(t, p.Permanent)
	require.Equal(t, int64(-1), p.Remaining)
	require.True(t, s.IsBanned("forever"))
	require.Len(t, s.BanList(), 1)
}

func TestServerBanExpiryLazyAndSweep(t *testing.T) {
	s := newServer()
	defer s.Close()

	now := time.Now().UnixNano()
	s.ApplyBan("gone", "old", now-2_000_000_000, now-1_000_000_000)    // expired 1s ago
	s.ApplyBan("stay", "fresh", now, now+10_000_000_000)               // active for 10s
	s.ApplyBan("forever", "perm", now-5_000_000_000, 0)                // permanent

	// expired generations are not enforced even before physical removal
	require.False(t, s.IsBanned("gone"))
	require.True(t, s.IsBanned("stay"))
	require.True(t, s.IsBanned("forever"))

	// the API only lists active policies
	require.Len(t, s.BanList(), 2)

	// sweep purges only expired temporary generations
	require.Equal(t, 1, s.sweepBans(now))
	require.False(t, s.IsBanned("gone"))
	require.True(t, s.IsBanned("stay"))
	require.True(t, s.IsBanned("forever"))

	// a stale generation cleanup never purges a newer re-ban
	require.False(t, s.RemoveBanGeneration("stay", now-9_000_000_000, now-8_000_000_000))
	require.True(t, s.IsBanned("stay"))
	require.True(t, s.RemoveBanGeneration("stay", now, now+10_000_000_000))
	require.False(t, s.IsBanned("stay"))
}

func TestServerApplyBanCAS(t *testing.T) {
	s := newServer()
	defer s.Close()

	now := time.Now().UnixNano()
	s.ApplyBan("c", "new", now, now+10_000_000_000)
	// a stale raft entry (older generation) must not downgrade the policy
	s.ApplyBan("c", "old", now-5_000_000_000, now-4_000_000_000)
	require.True(t, s.IsBanned("c"))
	list := s.BanList()
	require.Len(t, list, 1)
	require.Equal(t, "new", list[0].Reason)

	// an unban requested before this ban was created must not remove it
	s.ApplyBanRemoval("c", now-6_000_000_000)
	require.True(t, s.IsBanned("c"))

	// unban requested afterwards removes it
	s.ApplyBanRemoval("c", now+1_000_000_000)
	require.False(t, s.IsBanned("c"))
}

func TestEstablishConnectionBannedRejected(t *testing.T) {
	s := newServer()
	defer s.Close()

	cid := packets.TPacketData[packets.Connect].Get(packets.TConnectClean).Packet.Connect.ClientIdentifier
	require.NotEmpty(t, cid)
	s.BanClient(cid, "test ban", time.Minute)

	r, w := net.Pipe()
	o := make(chan error, 1)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
	}()

	recv := make(chan []byte, 1)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	err := <-o
	require.Error(t, err)
	require.Contains(t, err.Error(), "banned client")

	// a banned connection is closed without a connack
	require.Empty(t, <-recv)

	_ = w.Close()
	_ = r.Close()
}

func TestEstablishConnectionAfterUnbanAccepted(t *testing.T) {
	s := newServer()
	defer s.Close()

	cid := packets.TPacketData[packets.Connect].Get(packets.TConnectClean).Packet.Connect.ClientIdentifier
	s.BanClient(cid, "test ban", time.Millisecond)
	require.True(t, s.IsBanned(cid))
	// after expiry the client is no longer rejected
	time.Sleep(5 * time.Millisecond)
	require.False(t, s.IsBanned(cid))

	r, w := net.Pipe()
	o := make(chan error, 1)
	go func() {
		o <- s.EstablishConnection("tcp", r)
	}()

	go func() {
		_, _ = w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectClean).RawBytes)
		_, _ = w.Write(packets.TPacketData[packets.Disconnect].Get(packets.TDisconnect).RawBytes)
	}()

	recv := make(chan []byte, 1)
	go func() {
		buf, err := io.ReadAll(w)
		require.NoError(t, err)
		recv <- buf
	}()

	require.NoError(t, <-o)
	require.Equal(t, packets.TPacketData[packets.Connack].Get(packets.TConnackAcceptedNoSession).RawBytes, <-recv)

	_ = w.Close()
	_ = r.Close()
}
