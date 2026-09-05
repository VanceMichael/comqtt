package system

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClone(t *testing.T) {
	o := &Info{
		Version:               "version",
		Started:               1,
		Time:                  2,
		Uptime:                3,
		BytesReceived:         4,
		BytesSent:             5,
		ClientsConnected:      6,
		ClientsMaximum:        7,
		ClientsTotal:          8,
		ClientsDisconnected:   9,
		MessagesReceived:      10,
		MessagesSent:          11,
		MessagesDropped:       20,
		Retained:              12,
		Inflight:              13,
		InflightDropped:       14,
		Subscriptions:         15,
		PacketsReceived:       16,
		PacketsSent:           17,
		MemoryAlloc:           18,
		Threads:               19,
		EnhancedAuthPending:   2,
		EnhancedAuthSucceeded: 7,
		EnhancedAuthFailed:    3,
	}

	n := o.Clone()

	require.Equal(t, o, n)
}

func TestEnhancedAuthMetricsRegistered(t *testing.T) {
	i := &Info{}
	reg := i.RegisterPrometheus()

	atomic.StoreInt64(&i.EnhancedAuthPending, 1)
	atomic.StoreInt64(&i.EnhancedAuthSucceeded, 5)
	atomic.StoreInt64(&i.EnhancedAuthFailed, 2)

	families, err := reg.Gather()
	require.NoError(t, err)

	names := map[string]bool{}
	for _, f := range families {
		names[f.GetName()] = true
	}
	require.True(t, names["enhanced_auth_pending"])
	require.True(t, names["enhanced_auth_succeeded"])
	require.True(t, names["enhanced_auth_failed"])
}
