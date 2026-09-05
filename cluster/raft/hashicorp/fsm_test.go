package hashicorp

import (
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/wind-c/comqtt/v2/cluster/message"
	base "github.com/wind-c/comqtt/v2/cluster/raft"
	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// testSnapshotSink captures the bytes an FSM persists so a snapshot can be
// round-tripped through Restore without a running raft node.
type testSnapshotSink struct {
	bytes.Buffer
}

func (s *testSnapshotSink) ID() string    { return "test" }
func (s *testSnapshotSink) Cancel() error { return nil }
func (s *testSnapshotSink) Close() error  { return nil }

// A snapshot must survive a Persist -> Restore round trip: the routing
// table has to be readable from the restored FSM, and any state the
// restoring node held before the snapshot must be discarded.
func TestFsmSnapshotRestoreRoundTrip(t *testing.T) {
	src := NewFsm(nil)
	src.Add("topic/a", "node1")
	src.Add("topic/a", "node2")
	src.Add("$share/g1/topic/b", "node3")

	snap, err := src.Snapshot()
	require.NoError(t, err)
	sink := new(testSnapshotSink)
	require.NoError(t, snap.Persist(sink))

	notifyCh := make(chan *message.Message, 8)
	dst := NewFsm(notifyCh)
	dst.Add("topic/stale", "nodeX")

	require.NoError(t, dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))

	require.ElementsMatch(t, []string{"node1", "node2"}, dst.Lookup("topic/a"))
	require.ElementsMatch(t, []string{"node3"}, dst.Lookup("$share/g1/topic/b"))
	require.Empty(t, dst.Lookup("topic/stale"))

	// restored filters are replayed so the local subscription tree can be rebuilt
	require.Equal(t, 2, len(notifyCh))
	for len(notifyCh) > 0 {
		msg := <-notifyCh
		require.Equal(t, packets.Subscribe, msg.Type)
		require.NotEmpty(t, msg.Payload)
	}
}

// Ban generations must be persisted in the snapshot and replayed when
// restored, so that restarted/caught-up nodes converge to the same policies.
func TestFsmSnapshotBansRoundTrip(t *testing.T) {
	src := NewFsm(nil)
	src.Add("topic/a", "node1")
	payload, err := json.Marshal(base.Ban{ClientID: "bad-client", Reason: "abuse", CreatedAt: 1000, ExpiresAt: 2000})
	require.NoError(t, err)
	m := message.Message{Type: message.BanAdd, ClientID: "bad-client", Payload: payload}
	src.Apply(&raft.Log{Data: m.MsgpackBytes()})

	snap, err := src.Snapshot()
	require.NoError(t, err)
	sink := new(testSnapshotSink)
	require.NoError(t, snap.Persist(sink))

	notifyCh := make(chan *message.Message, 16)
	dst := NewFsm(notifyCh)
	require.NoError(t, dst.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))))

	b, ok := dst.bans.Get("bad-client")
	require.True(t, ok)
	require.Equal(t, "abuse", b.Reason)
	require.Equal(t, int64(2000), b.ExpiresAt)

	var banReplays int
	for len(notifyCh) > 0 {
		msg := <-notifyCh
		if msg.Type == message.BanAdd {
			banReplays++
			require.Equal(t, "bad-client", msg.ClientID)
		}
	}
	require.Equal(t, 1, banReplays)
}

// A corrupt snapshot must leave the existing state untouched.
func TestFsmRestoreCorruptSnapshot(t *testing.T) {
	src := NewFsm(nil)
	src.Add("topic/a", "node1")

	snap, err := src.Snapshot()
	require.NoError(t, err)
	sink := new(testSnapshotSink)
	require.NoError(t, snap.Persist(sink))

	dst := NewFsm(nil)
	dst.Add("topic/b", "node2")

	truncated := sink.Bytes()[:len(sink.Bytes())/2]
	require.Error(t, dst.Restore(io.NopCloser(bytes.NewReader(truncated))))
	require.ElementsMatch(t, []string{"node2"}, dst.Lookup("topic/b"))
	require.Empty(t, dst.Lookup("topic/a"))
}
