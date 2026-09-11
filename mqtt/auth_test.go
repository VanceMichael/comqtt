// SPDX-License-Identifier: MIT
// SPDX-FileContributor: comqtt enhanced authentication

package mqtt

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

const testAuthMethod = "SCRAM-SHA-256"

// scriptedEnhancedAuthHook drives the enhanced authentication exchange with a
// test callback and records lifecycle events.
type scriptedEnhancedAuthHook struct {
	HookBase
	fn func(cl *Client, pk packets.Packet, call int) (packets.Packet, error)

	mu           sync.Mutex
	calls        int
	received     []packets.Packet
	states       []AuthState
	establishing int
	established  int
	disconnects  int
}

func (h *scriptedEnhancedAuthHook) ID() string { return "scripted-enhanced-auth" }

func (h *scriptedEnhancedAuthHook) Provides(b byte) bool {
	return bytes.Contains([]byte{
		OnAuthPacket,
		OnAuthStateChange,
		OnACLCheck,
		OnSessionEstablish,
		OnSessionEstablished,
		OnDisconnect,
	}, []byte{b})
}

func (h *scriptedEnhancedAuthHook) OnACLCheck(cl *Client, topic string, write bool) bool { return true }

func (h *scriptedEnhancedAuthHook) OnAuthPacket(cl *Client, pk packets.Packet) (packets.Packet, error) {
	h.mu.Lock()
	idx := h.calls
	h.calls++
	h.received = append(h.received, pk)
	fn := h.fn
	h.mu.Unlock()

	if fn == nil {
		return packets.Packet{}, nil
	}
	return fn(cl, pk, idx)
}

func (h *scriptedEnhancedAuthHook) OnAuthStateChange(cl *Client, state AuthState, method string, reason packets.Code) {
	h.mu.Lock()
	h.states = append(h.states, state)
	h.mu.Unlock()
}

func (h *scriptedEnhancedAuthHook) OnSessionEstablish(cl *Client, pk packets.Packet) {
	h.mu.Lock()
	h.establishing++
	h.mu.Unlock()
}

func (h *scriptedEnhancedAuthHook) OnSessionEstablished(cl *Client, pk packets.Packet) {
	h.mu.Lock()
	h.established++
	h.mu.Unlock()
}

func (h *scriptedEnhancedAuthHook) OnDisconnect(cl *Client, err error, expire bool) {
	h.mu.Lock()
	h.disconnects++
	h.mu.Unlock()
}

func (h *scriptedEnhancedAuthHook) snapshot() (states []AuthState, types []byte, establishing, established, disconnects int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	states = append([]AuthState{}, h.states...)
	for _, pk := range h.received {
		types = append(types, pk.FixedHeader.Type)
	}
	return states, types, h.establishing, h.established, h.disconnects
}

// --- packet builders (client -> broker) ---

func v5Connect(id, method string, data []byte, persistent bool) packets.Packet {
	pk := packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Connect},
		ProtocolVersion: 5,
		Connect: packets.ConnectParams{
			ProtocolName:     []byte("MQTT"),
			ClientIdentifier: id,
			Clean:            !persistent,
			Keepalive:        60,
		},
		Properties: packets.Properties{
			AuthenticationMethod: method,
			AuthenticationData:   data,
		},
	}
	if persistent {
		pk.Properties.SessionExpiryInterval = 300
		pk.Properties.SessionExpiryIntervalFlag = true
	}
	return pk
}

func v5ConnectPlain(id string, persistent bool) packets.Packet {
	pk := v5Connect(id, "", nil, persistent)
	pk.Properties.AuthenticationMethod = ""
	return pk
}

func v5Auth(rc byte, method string, data []byte) packets.Packet {
	return packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Auth},
		ProtocolVersion: 5,
		ReasonCode:      rc,
		Properties: packets.Properties{
			AuthenticationMethod: method,
			AuthenticationData:   data,
		},
	}
}

func v5Subscribe(packetID uint16, filter string) packets.Packet {
	return packets.Packet{
		// SUBSCRIBE fixed header reserved bits MUST be 0010 (QoS 1) [MQTT-3.8.1-1].
		FixedHeader:     packets.FixedHeader{Type: packets.Subscribe, Qos: 1},
		ProtocolVersion: 5,
		PacketID:        packetID,
		Filters:         packets.Subscriptions{{Filter: filter, Qos: 0}},
	}
}

func v5PublishQos0(topic string, payload []byte) packets.Packet {
	return packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Publish, Qos: 0},
		ProtocolVersion: 5,
		TopicName:       topic,
		Payload:         payload,
	}
}

func v5Pingreq() packets.Packet {
	return packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Pingreq},
		ProtocolVersion: 5,
	}
}

func v5Disconnect(rc byte) packets.Packet {
	return packets.Packet{
		FixedHeader:     packets.FixedHeader{Type: packets.Disconnect},
		ProtocolVersion: 5,
		ReasonCode:      rc,
	}
}

func mustEncodePacket(t *testing.T, pk packets.Packet) []byte {
	t.Helper()
	var buf bytes.Buffer
	var err error
	switch pk.FixedHeader.Type {
	case packets.Connect:
		err = pk.ConnectEncode(&buf)
	case packets.Auth:
		err = pk.AuthEncode(&buf)
	case packets.Subscribe:
		err = pk.SubscribeEncode(&buf)
	case packets.Unsubscribe:
		err = pk.UnsubscribeEncode(&buf)
	case packets.Publish:
		err = pk.PublishEncode(&buf)
	case packets.Pingreq:
		err = pk.PingreqEncode(&buf)
	case packets.Disconnect:
		err = pk.DisconnectEncode(&buf)
	default:
		t.Fatalf("unsupported encode packet type %d", pk.FixedHeader.Type)
	}
	require.NoError(t, err)
	return buf.Bytes()
}

// --- frame reader (broker -> client) ---

type frameReader struct {
	r *bufio.Reader
}

func newFrameReader(c net.Conn) *frameReader {
	return &frameReader{r: bufio.NewReader(c)}
}

func (f *frameReader) next() (packets.Packet, error) {
	hdr, err := f.r.ReadByte()
	if err != nil {
		return packets.Packet{}, err
	}
	fh := new(packets.FixedHeader)
	if err := fh.Decode(hdr); err != nil {
		return packets.Packet{}, err
	}
	rem, _, err := packets.DecodeLength(f.r)
	if err != nil {
		return packets.Packet{}, err
	}
	fh.Remaining = rem // decoders (e.g. Disconnect) gate on FixedHeader.Remaining
	payload := make([]byte, rem)
	if _, err := io.ReadFull(f.r, payload); err != nil {
		return packets.Packet{}, err
	}
	pk := packets.Packet{FixedHeader: *fh, ProtocolVersion: 5}
	switch fh.Type {
	case packets.Auth:
		err = pk.AuthDecode(payload)
	case packets.Connack:
		err = pk.ConnackDecode(payload)
	case packets.Disconnect:
		err = pk.DisconnectDecode(payload)
	case packets.Suback:
		err = pk.SubackDecode(payload)
	case packets.Puback:
		err = pk.PubackDecode(payload)
	case packets.Pingresp:
		// no payload
	default:
		return pk, nil
	}
	return pk, err
}

func readFrame(t *testing.T, fr *frameReader) packets.Packet {
	t.Helper()
	pk, err := fr.next()
	require.NoError(t, err)
	return pk
}

type frameResult struct{ pk packets.Packet }

func drainFrames(w net.Conn) <-chan frameResult {
	fr := newFrameReader(w)
	ch := make(chan frameResult, 64)
	go func() {
		for {
			pk, err := fr.next()
			if err != nil {
				close(ch)
				return
			}
			ch <- frameResult{pk: pk}
		}
	}()
	return ch
}

// --- harness ---

func newEnhancedServer(t *testing.T, hook *scriptedEnhancedAuthHook) *Server {
	s := newServer()
	require.NoError(t, s.AddHook(hook, nil))
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func dialServer(t *testing.T, s *Server) (net.Conn, chan error) {
	r, w := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()
	return w, done
}

func waitEstablish(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("EstablishConnection did not return in time")
		return nil
	}
}

func writePacket(t *testing.T, w net.Conn, pk packets.Packet) {
	t.Helper()
	_, err := w.Write(mustEncodePacket(t, pk))
	require.NoError(t, err)
}

// challengeOnConnect is the standard two-round script: CONNECT -> AUTH 0x18,
// client AUTH 0x18 -> AUTH 0x00.
func challengeOnConnect(cl *Client, pk packets.Packet, call int) (packets.Packet, error) {
	if call == 0 {
		return v5Auth(packets.CodeContinueAuthentication.Code, testAuthMethod, []byte("server-challenge")), nil
	}
	return v5Auth(packets.CodeSuccess.Code, testAuthMethod, []byte("server-final")), nil
}

// --- AC-1: multi-round handshake succeeds, then business packets flow ---

func TestEnhancedAuthTwoRoundHandshake(t *testing.T) {
	hook := &scriptedEnhancedAuthHook{fn: challengeOnConnect}
	s := newEnhancedServer(t, hook)

	w, done := dialServer(t, s)
	defer w.Close()
	fr := newFrameReader(w)

	writePacket(t, w, v5Connect("ea-two-round", testAuthMethod, []byte("client-first"), false))

	a1 := readFrame(t, fr)
	require.Equal(t, packets.Auth, a1.FixedHeader.Type)
	require.Equal(t, packets.CodeContinueAuthentication.Code, a1.ReasonCode)
	require.Equal(t, testAuthMethod, a1.Properties.AuthenticationMethod)
	require.Equal(t, []byte("server-challenge"), a1.Properties.AuthenticationData)

	writePacket(t, w, v5Auth(packets.CodeContinueAuthentication.Code, testAuthMethod, []byte("client-final")))

	a2 := readFrame(t, fr)
	require.Equal(t, packets.Auth, a2.FixedHeader.Type)
	require.Equal(t, packets.CodeSuccess.Code, a2.ReasonCode)
	require.Equal(t, []byte("server-final"), a2.Properties.AuthenticationData)

	ca := readFrame(t, fr)
	require.Equal(t, packets.Connack, ca.FixedHeader.Type)
	require.Equal(t, packets.CodeSuccess.Code, ca.ReasonCode)
	require.False(t, ca.SessionPresent)
	require.Equal(t, testAuthMethod, ca.Properties.AuthenticationMethod)

	// business packets are allowed only after CONNACK
	writePacket(t, w, v5Subscribe(1, "a/b/c"))
	sb := readFrame(t, fr)
	require.Equal(t, packets.Suback, sb.FixedHeader.Type)
	require.Equal(t, []byte{packets.CodeSuccess.Code}, sb.ReasonCodes)

	cl, ok := s.Clients.Get("ea-two-round")
	require.True(t, ok)
	require.Equal(t, AuthStateAuthenticated, cl.AuthState())
	require.Equal(t, 1, len(cl.State.Subscriptions.GetAll()))

	// clean disconnect ends the connection
	writePacket(t, w, v5Disconnect(packets.CodeDisconnect.Code))
	require.NoError(t, waitEstablish(t, done))

	states, types, establishing, established, disconnects := hook.snapshot()
	require.Equal(t, []byte{packets.Connect, packets.Auth}, types)
	require.Equal(t, []AuthState{AuthStatePending, AuthStateAuthenticated}, states)
	require.Equal(t, 1, establishing)
	require.Equal(t, 1, established)
	require.Equal(t, 1, disconnects, "clean disconnect after a completed handshake fires OnDisconnect")
	require.Equal(t, int64(1), atomic.LoadInt64(&s.Info.EnhancedAuthSucceeded))
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthPending))
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthFailed))
}

// --- AC-2: immediate success variants establish the session exactly once ---

func TestEnhancedAuthImmediateSuccess(t *testing.T) {
	tests := []struct {
		name     string
		resp     packets.Packet
		wantAuth bool
	}{
		{name: "zero packet, no auth frame", resp: packets.Packet{}, wantAuth: false},
		{name: "auth success frame", resp: v5Auth(packets.CodeSuccess.Code, testAuthMethod, []byte("final")), wantAuth: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hook := &scriptedEnhancedAuthHook{fn: func(cl *Client, pk packets.Packet, call int) (packets.Packet, error) {
				return tc.resp, nil
			}}
			s := newEnhancedServer(t, hook)

			w, done := dialServer(t, s)
			defer w.Close()
			fr := newFrameReader(w)

			writePacket(t, w, v5Connect("ea-immediate", testAuthMethod, nil, false))

			if tc.wantAuth {
				a := readFrame(t, fr)
				require.Equal(t, packets.Auth, a.FixedHeader.Type)
				require.Equal(t, packets.CodeSuccess.Code, a.ReasonCode)
			}
			ca := readFrame(t, fr)
			require.Equal(t, packets.Connack, ca.FixedHeader.Type)
			require.Equal(t, packets.CodeSuccess.Code, ca.ReasonCode)

			writePacket(t, w, v5Disconnect(packets.CodeDisconnect.Code))
			require.NoError(t, waitEstablish(t, done))

			states, types, establishing, established, disconnects := hook.snapshot()
			require.Equal(t, []byte{packets.Connect}, types) // no AUTH packets
			require.Equal(t, []AuthState{AuthStatePending, AuthStateAuthenticated}, states)
			require.Equal(t, 1, establishing)
			require.Equal(t, 1, established)
			require.Equal(t, 1, disconnects)
		})
	}
}

// --- AC-3: business packets during the handshake are rejected deterministically ---

func TestEnhancedAuthBusinessPacketDuringHandshake(t *testing.T) {
	offending := []struct {
		name string
		pk   packets.Packet
	}{
		{"publish", v5PublishQos0("a/b/c", []byte("x"))},
		{"subscribe", v5Subscribe(1, "a/b/c")},
		{"pingreq", v5Pingreq()},
	}
	for _, tc := range offending {
		t.Run(tc.name, func(t *testing.T) {
			hook := &scriptedEnhancedAuthHook{fn: challengeOnConnect}
			s := newEnhancedServer(t, hook)

			w, done := dialServer(t, s)
			defer w.Close()
			fr := newFrameReader(w)

			writePacket(t, w, v5Connect("ea-gate-"+tc.name, testAuthMethod, nil, false))
			challenge := readFrame(t, fr)
			require.Equal(t, packets.CodeContinueAuthentication.Code, challenge.ReasonCode)

			writePacket(t, w, tc.pk)

			d := readFrame(t, fr)
			require.Equal(t, packets.Disconnect, d.FixedHeader.Type)
			require.Equal(t, packets.ErrProtocolViolation.Code, d.ReasonCode) // 0x82

			require.Error(t, waitEstablish(t, done))

			// no business side effects
			require.Equal(t, 0, s.Clients.Len())
			require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.Subscriptions))
			_, types, establishing, established, disconnects := hook.snapshot()
			require.Equal(t, []byte{packets.Connect}, types) // hook never saw the offending packet as AUTH
			require.Equal(t, 0, establishing)
			require.Equal(t, 0, established)
			require.Equal(t, 1, disconnects, "rejected handshake fires OnDisconnect")
		})
	}
}

// --- AC-4: method mismatch -> 0x8C; missing method -> 0x82 ---

func TestEnhancedAuthMethodViolations(t *testing.T) {
	tests := []struct {
		name       string
		auth       packets.Packet
		wantReason byte
	}{
		{"different method", v5Auth(packets.CodeContinueAuthentication.Code, "OTHER-METHOD", nil), packets.ErrBadAuthenticationMethod.Code},
		{"missing method", v5Auth(packets.CodeContinueAuthentication.Code, "", nil), packets.ErrProtocolViolation.Code},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hook := &scriptedEnhancedAuthHook{fn: challengeOnConnect}
			s := newEnhancedServer(t, hook)

			w, done := dialServer(t, s)
			defer w.Close()
			fr := newFrameReader(w)

			writePacket(t, w, v5Connect("ea-method-"+tc.name, testAuthMethod, nil, false))
			_ = readFrame(t, fr) // challenge

			writePacket(t, w, tc.auth)

			d := readFrame(t, fr)
			require.Equal(t, packets.Disconnect, d.FixedHeader.Type)
			require.Equal(t, tc.wantReason, d.ReasonCode)
			require.Error(t, waitEstablish(t, done))
		})
	}
}

// --- AC-5: client reason codes out of the handshake phase ---

func TestEnhancedAuthIllegalReasonDuringHandshake(t *testing.T) {
	tests := []struct {
		name string
		rc   byte
	}{
		{"reauth code during handshake", packets.CodeReAuthenticate.Code}, // 0x19
		{"success code from client", packets.CodeSuccess.Code},            // 0x00 is server->client only
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hook := &scriptedEnhancedAuthHook{fn: challengeOnConnect}
			s := newEnhancedServer(t, hook)

			w, done := dialServer(t, s)
			defer w.Close()
			fr := newFrameReader(w)

			writePacket(t, w, v5Connect("ea-rc-"+tc.name, testAuthMethod, nil, false))
			_ = readFrame(t, fr)

			writePacket(t, w, v5Auth(tc.rc, testAuthMethod, nil))

			d := readFrame(t, fr)
			require.Equal(t, packets.Disconnect, d.FixedHeader.Type)
			require.Equal(t, packets.ErrProtocolViolation.Code, d.ReasonCode)
			require.Error(t, waitEstablish(t, done))
		})
	}
}

// --- AC-6: hook denial at the CONNECT stage is signalled with CONNACK ---

func TestEnhancedAuthConnectHookDenies(t *testing.T) {
	tests := []struct {
		name string
		code packets.Code
	}{
		{"not authorized", packets.ErrNotAuthorized},                      // 0x87
		{"bad authentication method", packets.ErrBadAuthenticationMethod}, // 0x8C
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			hook := &scriptedEnhancedAuthHook{fn: func(cl *Client, pk packets.Packet, call int) (packets.Packet, error) {
				return packets.Packet{}, tc.code
			}}
			s := newEnhancedServer(t, hook)

			w, done := dialServer(t, s)
			defer w.Close()
			fr := newFrameReader(w)

			writePacket(t, w, v5Connect("ea-deny-"+tc.name, testAuthMethod, nil, false))

			ca := readFrame(t, fr)
			require.Equal(t, packets.Connack, ca.FixedHeader.Type)
			require.Equal(t, tc.code.Code, ca.ReasonCode)

			err := waitEstablish(t, done)
			require.Error(t, err)

			require.Equal(t, 0, s.Clients.Len())
			require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthSucceeded))
			require.Equal(t, int64(1), atomic.LoadInt64(&s.Info.EnhancedAuthFailed))
			states, _, establishing, _, disconnects := hook.snapshot()
			require.Equal(t, []AuthState{AuthStatePending, AuthStateFailed}, states)
			require.Equal(t, 0, establishing)
			require.Equal(t, 1, disconnects, "CONNECT-stage refusal fires OnDisconnect")
		})
	}
}

// --- AC-7: challenge write failure terminates deterministically ---

func TestEnhancedAuthChallengeWriteFailure(t *testing.T) {
	hook := &scriptedEnhancedAuthHook{fn: challengeOnConnect}
	s := newEnhancedServer(t, hook)

	w, done := dialServer(t, s)

	// write CONNECT and close the client side before the broker can deliver
	// the challenge.
	_, err := w.Write(mustEncodePacket(t, v5Connect("ea-writefail", testAuthMethod, nil, false)))
	require.NoError(t, err)
	require.NoError(t, w.Close())

	require.Error(t, waitEstablish(t, done))
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthPending))
	require.Equal(t, int64(1), atomic.LoadInt64(&s.Info.EnhancedAuthFailed))
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthSucceeded))
}

// --- AC-8: re-authentication succeeds without touching the session ---

func TestReAuthSuccessSessionIntact(t *testing.T) {
	call := atomic.Int32{}
	hook := &scriptedEnhancedAuthHook{fn: func(cl *Client, pk packets.Packet, c int) (packets.Packet, error) {
		switch pk.FixedHeader.Type {
		case packets.Connect:
			return packets.Packet{}, nil // immediate success
		case packets.Auth:
			n := call.Add(1)
			if n == 1 {
				return v5Auth(packets.CodeContinueAuthentication.Code, testAuthMethod, []byte("reauth-challenge")), nil
			}
			return v5Auth(packets.CodeSuccess.Code, testAuthMethod, []byte("reauth-final")), nil
		}
		return packets.Packet{}, nil
	}}
	s := newEnhancedServer(t, hook)

	w, done := dialServer(t, s)
	defer w.Close()
	fr := newFrameReader(w)

	writePacket(t, w, v5Connect("ea-reauth", testAuthMethod, nil, false))
	require.Equal(t, packets.CodeSuccess.Code, readFrame(t, fr).ReasonCode)

	writePacket(t, w, v5Subscribe(1, "a/b"))
	require.Equal(t, packets.Suback, readFrame(t, fr).FixedHeader.Type)

	cl, ok := s.Clients.Get("ea-reauth")
	require.True(t, ok)
	require.Equal(t, 1, len(cl.State.Subscriptions.GetAll()))

	// seed an in-flight QoS message to prove re-authentication must not touch
	// inflight delivery state either
	inflightBefore := *packets.TPacketData[packets.Publish].Get(packets.TPublishQos1).Packet
	inflightBefore.PacketID = 42
	cl.State.Inflight.Set(inflightBefore)
	require.Equal(t, 1, cl.State.Inflight.Len())

	// client initiates re-authentication
	writePacket(t, w, v5Auth(packets.CodeReAuthenticate.Code, testAuthMethod, []byte("reauth-start")))
	challenge := readFrame(t, fr)
	require.Equal(t, packets.Auth, challenge.FixedHeader.Type)
	require.Equal(t, packets.CodeContinueAuthentication.Code, challenge.ReasonCode)

	// business packets keep flowing while re-authentication is in progress
	writePacket(t, w, v5Pingreq())
	pong := readFrame(t, fr)
	require.Equal(t, packets.Pingresp, pong.FixedHeader.Type)

	// client completes the exchange
	writePacket(t, w, v5Auth(packets.CodeContinueAuthentication.Code, testAuthMethod, []byte("reauth-response")))
	final := readFrame(t, fr)
	require.Equal(t, packets.Auth, final.FixedHeader.Type)
	require.Equal(t, packets.CodeSuccess.Code, final.ReasonCode)

	// session untouched: subscriptions AND the in-flight message survive re-auth
	require.Equal(t, AuthStateAuthenticated, cl.AuthState())
	require.Equal(t, 1, len(cl.State.Subscriptions.GetAll()))
	require.Equal(t, 1, cl.State.Inflight.Len(), "inflight messages must survive re-authentication")
	_, stillInflight := cl.State.Inflight.Get(42)
	require.True(t, stillInflight)

	writePacket(t, w, v5Disconnect(packets.CodeDisconnect.Code))
	require.NoError(t, waitEstablish(t, done))

	states, _, establishing, established, disconnects := hook.snapshot()
	require.Equal(t, []AuthState{
		AuthStatePending,
		AuthStateAuthenticated,
		AuthStateReAuthenticating,
		AuthStateAuthenticated,
	}, states)
	require.Equal(t, 1, establishing) // session established only for the initial connect
	require.Equal(t, 1, established)
	require.Equal(t, 1, disconnects)
	require.Equal(t, int64(2), atomic.LoadInt64(&s.Info.EnhancedAuthSucceeded))
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthPending))
}

// --- AC-9: re-authentication failure disconnects but keeps the persistent session ---

func TestReAuthFailurePreservesPersistentSession(t *testing.T) {
	hook := &scriptedEnhancedAuthHook{fn: func(cl *Client, pk packets.Packet, call int) (packets.Packet, error) {
		switch pk.FixedHeader.Type {
		case packets.Connect:
			return packets.Packet{}, nil // immediate success
		case packets.Auth:
			return packets.Packet{}, packets.ErrNotAuthorized // re-authentication fails
		}
		return packets.Packet{}, nil
	}}
	s := newEnhancedServer(t, hook)

	// first connection: enhanced auth, persistent, subscribed
	w1, done1 := dialServer(t, s)
	fr1 := newFrameReader(w1)
	writePacket(t, w1, v5Connect("ea-reauth-fail", testAuthMethod, nil, true))
	require.Equal(t, packets.CodeSuccess.Code, readFrame(t, fr1).ReasonCode)
	writePacket(t, w1, v5Subscribe(1, "p/ersist"))
	require.Equal(t, packets.Suback, readFrame(t, fr1).FixedHeader.Type)

	// re-authentication starts and fails
	writePacket(t, w1, v5Auth(packets.CodeReAuthenticate.Code, testAuthMethod, nil))
	d := readFrame(t, fr1)
	require.Equal(t, packets.Disconnect, d.FixedHeader.Type)
	require.Equal(t, packets.ErrNotAuthorized.Code, d.ReasonCode)
	require.Error(t, waitEstablish(t, done1))
	require.NoError(t, w1.Close())

	// persistent session remains registered with its subscriptions
	old, ok := s.Clients.Get("ea-reauth-fail")
	require.True(t, ok)
	require.Equal(t, 1, len(old.State.Subscriptions.GetAll()))
	require.Equal(t, AuthStateFailed, old.AuthState())

	// reconnect with the same client id and authenticate successfully
	w2, done2 := dialServer(t, s)
	defer w2.Close()
	fr2 := newFrameReader(w2)
	writePacket(t, w2, v5Connect("ea-reauth-fail", testAuthMethod, nil, true))
	ca := readFrame(t, fr2)
	require.Equal(t, packets.Connack, ca.FixedHeader.Type)
	require.Equal(t, packets.CodeSuccess.Code, ca.ReasonCode)
	require.True(t, ca.SessionPresent, "persistent session must resume after re-auth failure")

	newCl, ok := s.Clients.Get("ea-reauth-fail")
	require.True(t, ok)
	require.Equal(t, AuthStateAuthenticated, newCl.AuthState())
	require.Equal(t, 1, len(newCl.State.Subscriptions.GetAll()), "subscriptions inherited")

	writePacket(t, w2, v5Disconnect(packets.CodeDisconnect.Code))
	require.NoError(t, waitEstablish(t, done2))
}

// --- AC-10: illegal AUTH packets in invalid phases ---

func TestEnhancedAuthIllegalAuthPlainV5(t *testing.T) {
	// plain v5 connection (no Authentication Method, legacy path): any AUTH is
	// a protocol violation.
	s := newServer()
	t.Cleanup(func() { _ = s.Close() })

	w, done := dialServer(t, s)
	defer w.Close()
	fr := newFrameReader(w)

	// persistent so the client record survives the disconnect and can be inspected
	writePacket(t, w, v5ConnectPlain("plain-v5", true))
	require.Equal(t, packets.CodeSuccess.Code, readFrame(t, fr).ReasonCode)

	writePacket(t, w, v5Auth(packets.CodeContinueAuthentication.Code, testAuthMethod, nil))
	d := readFrame(t, fr)
	require.Equal(t, packets.Disconnect, d.FixedHeader.Type)
	require.Equal(t, packets.ErrProtocolViolation.Code, d.ReasonCode)
	require.Error(t, waitEstablish(t, done))

	cl, ok := s.Clients.Get("plain-v5")
	require.True(t, ok)
	require.Equal(t, AuthStateNone, cl.AuthState(), "plain connection never enters enhanced auth")
}

func TestEnhancedAuthV3AuthPacketRejected(t *testing.T) {
	// MQTT v3.1.1 has no AUTH packet: packet type 15 is reserved and the
	// broker must close the connection.
	s := newServer()
	t.Cleanup(func() { _ = s.Close() })

	r, w := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.EstablishConnection("tcp", r) }()
	defer w.Close()

	_, err := w.Write(packets.TPacketData[packets.Connect].Get(packets.TConnectMqtt311).RawBytes)
	require.NoError(t, err)

	// a v3 CONNACK is exactly 4 bytes with no properties block
	connack := make([]byte, 4)
	_, err = io.ReadFull(w, connack)
	require.NoError(t, err)
	require.Equal(t, []byte{packets.Connack << 4, 0x02, 0x00, 0x00}, connack)

	// send packet type 15 (AUTH in v5, reserved in v3)
	_, err = w.Write([]byte{packets.Auth << 4, 0x00})
	require.NoError(t, err)

	// the broker terminates the connection deterministically
	require.Error(t, waitEstablish(t, done))
}

func TestReAuthDuplicateStartAndBareContinue(t *testing.T) {
	hook := &scriptedEnhancedAuthHook{fn: func(cl *Client, pk packets.Packet, call int) (packets.Packet, error) {
		switch pk.FixedHeader.Type {
		case packets.Connect:
			return packets.Packet{}, nil
		case packets.Auth:
			return v5Auth(packets.CodeContinueAuthentication.Code, testAuthMethod, []byte("challenge")), nil
		}
		return packets.Packet{}, nil
	}}
	s := newEnhancedServer(t, hook)

	t.Run("duplicate re-authentication start", func(t *testing.T) {
		w, done := dialServer(t, s)
		defer w.Close()
		fr := newFrameReader(w)

		writePacket(t, w, v5Connect("ea-dup-reauth", testAuthMethod, nil, false))
		_ = readFrame(t, fr)

		writePacket(t, w, v5Auth(packets.CodeReAuthenticate.Code, testAuthMethod, nil))
		require.Equal(t, packets.CodeContinueAuthentication.Code, readFrame(t, fr).ReasonCode)

		// a second 0x19 while the exchange is already in progress
		writePacket(t, w, v5Auth(packets.CodeReAuthenticate.Code, testAuthMethod, nil))
		d := readFrame(t, fr)
		require.Equal(t, packets.Disconnect, d.FixedHeader.Type)
		require.Equal(t, packets.ErrProtocolViolation.Code, d.ReasonCode)
		require.Error(t, waitEstablish(t, done))
	})

	t.Run("bare continue without re-auth start", func(t *testing.T) {
		w, done := dialServer(t, s)
		defer w.Close()
		fr := newFrameReader(w)

		writePacket(t, w, v5Connect("ea-bare-cont", testAuthMethod, nil, false))
		_ = readFrame(t, fr)

		// 0x18 without a preceding 0x19
		writePacket(t, w, v5Auth(packets.CodeContinueAuthentication.Code, testAuthMethod, nil))
		d := readFrame(t, fr)
		require.Equal(t, packets.Disconnect, d.FixedHeader.Type)
		require.Equal(t, packets.ErrProtocolViolation.Code, d.ReasonCode)
		require.Error(t, waitEstablish(t, done))
	})
}

// --- AC-11: unauthenticated connections must not take over existing sessions ---

func TestEnhancedAuthNoTakeoverBeforeAuthenticated(t *testing.T) {
	connects := atomic.Int32{}
	hook := &scriptedEnhancedAuthHook{fn: func(cl *Client, pk packets.Packet, call int) (packets.Packet, error) {
		if pk.FixedHeader.Type == packets.Connect && connects.Add(1) == 2 {
			// the takeover attempt issues a challenge but never completes
			return v5Auth(packets.CodeContinueAuthentication.Code, testAuthMethod, []byte("challenge")), nil
		}
		return packets.Packet{}, nil // all other connects authenticate immediately
	}}
	s := newEnhancedServer(t, hook)

	// original connection: persistent, subscribed
	w1, done1 := dialServer(t, s)
	frames1 := drainFrames(w1)
	writePacket(t, w1, v5Connect("takeover-ea", testAuthMethod, nil, true))
	require.Equal(t, packets.Connack, (<-frames1).pk.FixedHeader.Type)
	writePacket(t, w1, v5Subscribe(1, "t/o"))
	require.Equal(t, packets.Suback, (<-frames1).pk.FixedHeader.Type)

	old, ok := s.Clients.Get("takeover-ea")
	require.True(t, ok)
	oldSubs := len(old.State.Subscriptions.GetAll())
	require.Equal(t, 1, oldSubs)

	// second connection: same client id, enhanced auth is abandoned mid-handshake
	w2, done2 := dialServer(t, s)
	fr2 := newFrameReader(w2)
	writePacket(t, w2, v5Connect("takeover-ea", testAuthMethod, nil, true))
	challenge, err := fr2.next()
	require.NoError(t, err)
	require.Equal(t, packets.CodeContinueAuthentication.Code, challenge.ReasonCode)
	require.NoError(t, w2.Close())
	require.Error(t, waitEstablish(t, done2))

	// original connection must survive the unauthenticated attempt
	require.False(t, old.Closed(), "existing client must not be taken over before auth success")
	require.Nil(t, old.StopCause())
	require.Equal(t, oldSubs, len(old.State.Subscriptions.GetAll()))
	require.Equal(t, old, mustGetClient(s, "takeover-ea"))

	// third connection: authenticates successfully -> single takeover
	w3, done3 := dialServer(t, s)
	fr3 := newFrameReader(w3)
	writePacket(t, w3, v5Connect("takeover-ea", testAuthMethod, nil, true))
	ca, err := fr3.next()
	require.NoError(t, err)
	require.Equal(t, packets.Connack, ca.FixedHeader.Type)
	require.True(t, ca.SessionPresent)

	// old connection received the takeover DISCONNECT
selectFrames:
	for {
		select {
		case f, ok := <-frames1:
			if !ok {
				t.Fatal("expected takeover DISCONNECT on old connection")
			}
			if f.pk.FixedHeader.Type == packets.Disconnect {
				require.Equal(t, packets.ErrSessionTakenOver.Code, f.pk.ReasonCode)
				break selectFrames
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for takeover DISCONNECT")
		}
	}
	require.Error(t, waitEstablish(t, done1))
	require.ErrorIs(t, old.StopCause(), packets.ErrSessionTakenOver)

	newCl := mustGetClient(s, "takeover-ea")
	require.NotSame(t, old, newCl)
	require.Equal(t, AuthStateAuthenticated, newCl.AuthState())
	require.Equal(t, 1, len(newCl.State.Subscriptions.GetAll()), "subscriptions inherited after authenticated takeover")

	writePacket(t, w3, v5Disconnect(packets.CodeDisconnect.Code))
	require.NoError(t, waitEstablish(t, done3))
	require.NoError(t, w1.Close())
}

func mustGetClient(s *Server, id string) *Client {
	cl, ok := s.Clients.Get(id)
	if !ok {
		return nil
	}
	return cl
}

// --- AC-12: CONNECT with method but no OnAuthPacket hook keeps legacy path ---

func TestEnhancedAuthLegacyFallbackWithoutHook(t *testing.T) {
	s := newServer() // only AllowHook (OnConnectAuthenticate/OnACLCheck), no OnAuthPacket
	t.Cleanup(func() { _ = s.Close() })

	w, done := dialServer(t, s)
	defer w.Close()
	fr := newFrameReader(w)

	writePacket(t, w, v5Connect("legacy-method", "SHA-1", []byte("auth-data"), false))
	ca := readFrame(t, fr)
	require.Equal(t, packets.Connack, ca.FixedHeader.Type)
	require.Equal(t, packets.CodeSuccess.Code, ca.ReasonCode)
	require.Equal(t, "SHA-1", ca.Properties.AuthenticationMethod, "legacy CONNACK echoes the method")

	cl, ok := s.Clients.Get("legacy-method")
	require.True(t, ok)
	require.Equal(t, AuthStateNone, cl.AuthState(), "legacy connection is not in enhanced auth")

	writePacket(t, w, v5Disconnect(packets.CodeDisconnect.Code))
	require.NoError(t, waitEstablish(t, done))
}

// --- AC-14: handshake abandoned by the client reconciles counters/state ---

func TestEnhancedAuthAbandonedHandshakeReconciles(t *testing.T) {
	hook := &scriptedEnhancedAuthHook{fn: challengeOnConnect}
	s := newEnhancedServer(t, hook)

	w, done := dialServer(t, s)
	fr := newFrameReader(w)
	writePacket(t, w, v5Connect("ea-abandon", testAuthMethod, nil, false))
	_ = readFrame(t, fr)

	// client vanishes mid-handshake without DISCONNECT
	require.NoError(t, w.Close())
	require.Error(t, waitEstablish(t, done))

	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthPending))
	require.Equal(t, int64(1), atomic.LoadInt64(&s.Info.EnhancedAuthFailed))
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthSucceeded))
	states, _, establishing, _, disconnects := hook.snapshot()
	require.Equal(t, []AuthState{AuthStatePending, AuthStateFailed}, states)
	require.Equal(t, 0, establishing)
	require.Equal(t, 1, disconnects, "interrupted handshake fires OnDisconnect")
}

// --- AC-3/AC-14: explicit DISCONNECT while pending aborts the handshake (non-success) ---

func TestEnhancedAuthExplicitDisconnectDuringHandshake(t *testing.T) {
	hook := &scriptedEnhancedAuthHook{fn: challengeOnConnect}
	s := newEnhancedServer(t, hook)

	w, done := dialServer(t, s)
	defer w.Close()
	fr := newFrameReader(w)

	writePacket(t, w, v5Connect("ea-abort-disc", testAuthMethod, nil, false))
	require.Equal(t, packets.CodeContinueAuthentication.Code, readFrame(t, fr).ReasonCode)

	// the client cleanly abandons the exchange with DISCONNECT (reason 0x00)
	writePacket(t, w, v5Disconnect(packets.CodeDisconnect.Code))

	// EstablishConnection must NOT return nil-success: no CONNACK/session and
	// the aborted exchange is counted as a failure.
	require.Error(t, waitEstablish(t, done))
	require.Equal(t, 0, s.Clients.Len(), "no session is established for an aborted handshake")
	require.Equal(t, int64(0), atomic.LoadInt64(&s.Info.EnhancedAuthPending))
	require.Equal(t, int64(1), atomic.LoadInt64(&s.Info.EnhancedAuthFailed))
	states, _, establishing, established, disconnects := hook.snapshot()
	require.Equal(t, 0, establishing)
	require.Equal(t, 0, established)
	require.Equal(t, 1, disconnects, "abandoned handshake fires OnDisconnect")
	require.Equal(t, AuthStateFailed, states[len(states)-1])
}

// --- AC-13: $SYS topics publish enhanced auth counters ---

func TestEnhancedAuthSYSTopics(t *testing.T) {
	hook := &scriptedEnhancedAuthHook{fn: func(cl *Client, pk packets.Packet, call int) (packets.Packet, error) {
		return packets.Packet{}, packets.ErrBadAuthenticationMethod
	}}
	s := newEnhancedServer(t, hook)

	w, done := dialServer(t, s)
	fr := newFrameReader(w)
	writePacket(t, w, v5Connect("ea-sys", testAuthMethod, nil, false))
	ca := readFrame(t, fr)
	require.Equal(t, packets.ErrBadAuthenticationMethod.Code, ca.ReasonCode)
	_ = waitEstablish(t, done)
	_ = w.Close()

	s.publishSysTopics()

	msgs := s.Topics.Messages(SysPrefix + "/broker/auth/enhanced/failed")
	require.Len(t, msgs, 1)
	require.Equal(t, "1", string(msgs[0].Payload))

	msgs = s.Topics.Messages(SysPrefix + "/broker/auth/enhanced/succeeded")
	require.Len(t, msgs, 1)
	require.Equal(t, "0", string(msgs[0].Payload))
}
