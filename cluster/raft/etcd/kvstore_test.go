package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wind-c/comqtt/v2/cluster/message"
	base "github.com/wind-c/comqtt/v2/cluster/raft"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// A snapshot must survive a getSnapshot -> recoverFromSnapshot round trip:
// the routing table has to be readable afterwards, and any state held
// before the snapshot must be discarded.
func TestKVStoreRecoverFromSnapshot(t *testing.T) {
	src := &KVStore{KV: base.NewKV(), bans: base.NewBanBook()}
	src.Add("topic/a", "node1")
	src.Add("topic/a", "node2")

	snapshot, err := src.getSnapshot()
	require.NoError(t, err)

	notifyCh := make(chan *message.Message, 8)
	dst := &KVStore{KV: base.NewKV(), bans: base.NewBanBook(), notifyCh: notifyCh}
	dst.Add("topic/stale", "nodeX")

	require.NoError(t, dst.recoverFromSnapshot(snapshot))
	require.ElementsMatch(t, []string{"node1", "node2"}, dst.Lookup("topic/a"))
	require.Empty(t, dst.Lookup("topic/stale"))

	// restored filters are replayed so the local subscription tree can be rebuilt
	require.Equal(t, 1, len(notifyCh))
	msg := <-notifyCh
	require.Equal(t, packets.Subscribe, msg.Type)
	require.Equal(t, "topic/a", string(msg.Payload))
	require.Equal(t, "node1,node2", msg.NodeID)
}

// Ban generations must travel with the snapshot so that nodes converging
// after a restart/partition re-enforce the same policies, and the replay
// must re-emit BanAdd notifications so downstream mirror/disconnect logic
// is re-armed.
func TestKVStoreSnapshotCarriesBans(t *testing.T) {
	src := &KVStore{KV: base.NewKV(), bans: base.NewBanBook()}
	src.Add("topic/a", "node1")
	src.bans.Put(base.Ban{ClientID: "bad-client", Reason: "abuse", CreatedAt: 1000, ExpiresAt: 2000})
	src.bans.Put(base.Ban{ClientID: "forever", CreatedAt: 1001, ExpiresAt: base.BanExpiresNever})

	snapshot, err := src.getSnapshot()
	require.NoError(t, err)

	notifyCh := make(chan *message.Message, 16)
	dst := &KVStore{KV: base.NewKV(), bans: base.NewBanBook(), notifyCh: notifyCh}
	require.NoError(t, dst.recoverFromSnapshot(snapshot))

	b, ok := dst.bans.Get("bad-client")
	require.True(t, ok)
	require.Equal(t, int64(2000), b.ExpiresAt)
	b2, ok := dst.bans.Get("forever")
	require.True(t, ok)
	require.True(t, b2.Permanent())

	// replays cover both subscriptions and ban generations
	var types []byte
	for len(notifyCh) > 0 {
		msg := <-notifyCh
		if msg.Type == message.BanAdd {
			types = append(types, byte(len(msg.Payload)))
			require.NotEmpty(t, msg.ClientID)
		}
	}
	require.Len(t, types, 2, "both bans must be replayed")
}

// A corrupt snapshot must leave the existing state untouched.
func TestKVStoreRecoverFromCorruptSnapshot(t *testing.T) {
	src := &KVStore{KV: base.NewKV(), bans: base.NewBanBook()}
	src.Add("topic/a", "node1")
	snapshot, err := src.getSnapshot()
	require.NoError(t, err)

	dst := &KVStore{KV: base.NewKV(), bans: base.NewBanBook()}
	dst.Add("topic/b", "node2")

	require.Error(t, dst.recoverFromSnapshot(snapshot[:len(snapshot)/2]))
	require.ElementsMatch(t, []string{"node2"}, dst.Lookup("topic/b"))
	require.Empty(t, dst.Lookup("topic/a"))
}
