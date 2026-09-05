// SPDX-License-Identifier: MIT
// SPDX-FileContributor: comqtt enhanced authentication

package mqtt

import (
	"errors"
	"sync/atomic"

	"github.com/wind-c/comqtt/v2/mqtt/packets"
)

// errEnhancedAuthCompleted is the sentinel returned by the enhanced
// authentication handshake read handler once the hook reports success and
// the session has been established. It stops the handshake read loop without
// being treated as a connection error.
var errEnhancedAuthCompleted = errors.New("enhanced authentication completed")

// errEnhancedAuthAborted is the sentinel returned when the client abandons
// the handshake by sending DISCONNECT before authentication succeeds. It
// stops the handshake read loop and routes teardown through the failure path
// (rather than being mistaken for a successful completion).
var errEnhancedAuthAborted = errors.New("enhanced authentication aborted")

// enhancedAuthRequested reports whether the CONNECT packet starts an MQTT v5
// enhanced authentication exchange that a hook participates in. When the
// client declares an Authentication Method but no hook provides OnAuthPacket,
// the legacy connect path is preserved (backward compatible opt-in).
func (s *Server) enhancedAuthRequested(cl *Client, pk packets.Packet) bool {
	// AUTH packets and the Authentication Method/Data properties exist only in
	// MQTT v5 (§3.15, §4.12); v3 connections never enter this path.
	return cl.Properties.ProtocolVersion == 5 &&
		pk.Properties.AuthenticationMethod != "" &&
		s.hooks.Provides(OnAuthPacket)
}

// beginEnhancedAuth drives the initial MQTT v5 enhanced authentication
// handshake (MQTT v5.0 §4.12). It marks the connection as pending, invokes the
// authentication hook with the CONNECT packet, writes the hook's first
// challenge (if any), and reads AUTH packets until the exchange succeeds or
// fails. Business packets are never processed in this phase.
func (s *Server) beginEnhancedAuth(cl *Client, connectPk packets.Packet) error {
	method := connectPk.Properties.AuthenticationMethod
	cl.setAuthMethod(method)
	cl.storeAuthState(AuthStatePending)
	atomic.AddInt64(&s.Info.EnhancedAuthPending, 1)
	s.hooks.OnAuthStateChange(cl, AuthStatePending, method, packets.CodeContinueAuthentication)
	s.Log.Info("enhanced authentication started",
		"client", cl.ID, "remote", cl.Net.Remote, "listener", cl.Net.Listener, "method", method)

	out, err := s.hooks.OnAuthPacket(cl, connectPk)
	if err != nil {
		// [MQTT-3.2.2-7] CONNECT-stage refusal is signalled with a CONNACK.
		return s.failHandshakeAuth(cl, authHookErrorCode(err), true)
	}

	success, err := s.writeAuthHookResponse(cl, out)
	if err != nil {
		var code packets.Code
		if errors.As(err, &code) {
			return s.failHandshakeAuth(cl, code, true)
		}
		// Challenge write failed: the connection is dead. No CONNACK/AUTH is
		// sent; the connection is terminated deterministically and counters
		// are reconciled on teardown.
		return err
	}

	if success {
		return s.completeHandshakeAuthNil(cl, connectPk)
	}

	// Challenge issued: exchange AUTH packets until success or failure.
	readErr := cl.Read(func(cl *Client, in packets.Packet) error {
		return s.handleHandshakeAuthPacket(cl, in, connectPk)
	})
	if errors.Is(readErr, errEnhancedAuthCompleted) {
		return nil
	}

	// The client abandoned the exchange (DISCONNECT) or the network dropped;
	// either is a non-success outcome (counters reconciled on teardown).
	return readErr
}

// completeHandshakeAuthNil runs completeHandshakeAuth and converts the
// internal completion sentinel into a nil result for the caller.
func (s *Server) completeHandshakeAuthNil(cl *Client, connectPk packets.Packet) error {
	if err := s.completeHandshakeAuth(cl, connectPk); err != nil && !errors.Is(err, errEnhancedAuthCompleted) {
		return err
	}
	return nil
}

// handleHandshakeAuthPacket processes a single inbound packet while the
// initial enhanced authentication handshake is in progress. Only AUTH and
// DISCONNECT packets are legal before CONNACK (MQTT v5.0 §4.12).
func (s *Server) handleHandshakeAuthPacket(cl *Client, pk, connectPk packets.Packet) error {
	switch pk.FixedHeader.Type {
	case packets.Disconnect:
		// Client abandons the authentication exchange; stop the handshake via
		// the abort sentinel so teardown follows the (non-success) failure path
		// instead of being mistaken for a completed handshake.
		if err := s.processDisconnect(cl, pk); err != nil {
			return err
		}
		return errEnhancedAuthAborted
	case packets.Auth:
	default:
		// Before CONNACK is sent, only AUTH and DISCONNECT packets may flow
		// during the enhanced authentication exchange (MQTT v5.0 §4.12).
		return s.failHandshakeAuth(cl, packets.ErrProtocolViolation, false)
	}

	// [MQTT-4.12.0-5] every AUTH packet in the exchange must carry the same
	// Authentication Method as declared in the CONNECT packet.
	if pk.Properties.AuthenticationMethod == "" {
		return s.failHandshakeAuth(cl, packets.ErrProtocolViolation, false)
	}
	if pk.Properties.AuthenticationMethod != cl.AuthenticationMethod() {
		return s.failHandshakeAuth(cl, packets.ErrBadAuthenticationMethod, false)
	}

	// During the initial handshake, client->server AUTH can only Continue
	// authentication (0x18). 0x00 is a server->client reason code and 0x19
	// (Re-authenticate) is only valid after the session is established.
	if pk.ReasonCode != packets.CodeContinueAuthentication.Code {
		return s.failHandshakeAuth(cl, packets.ErrProtocolViolation, false)
	}

	out, err := s.hooks.OnAuthPacket(cl, pk)
	if err != nil {
		// Post-CONNECT failures close the connection with DISCONNECT.
		return s.failHandshakeAuth(cl, authHookErrorCode(err), false)
	}

	success, err := s.writeAuthHookResponse(cl, out)
	if err != nil {
		var code packets.Code
		if errors.As(err, &code) {
			return s.failHandshakeAuth(cl, code, false)
		}
		// Write failure: connection is dead; terminate deterministically.
		return err
	}

	if success {
		return s.completeHandshakeAuth(cl, connectPk)
	}

	return nil
}

// writeAuthHookResponse validates and writes the AUTH packet returned by an
// enhanced authentication hook, for both the initial handshake and
// re-authentication. It returns success=true when the hook signals completion
// (AUTH 0x00, or a zero packet meaning success without a final AUTH packet).
func (s *Server) writeAuthHookResponse(cl *Client, out packets.Packet) (success bool, err error) {
	switch out.FixedHeader.Type {
	case packets.Auth:
		if out.ReasonCode != packets.CodeContinueAuthentication.Code &&
			out.ReasonCode != packets.CodeSuccess.Code {
			// A server->client AUTH can only Continue (0x18) or Succeed (0x00).
			s.Log.Error("enhanced auth hook returned an illegal AUTH reason code",
				"client", cl.ID, "reason_code", out.ReasonCode)
			return false, packets.ErrImplementationSpecificError
		}
		// [MQTT-4.12.0-5] server->client AUTH packets use the same
		// Authentication Method as the exchange.
		out.Properties.AuthenticationMethod = cl.AuthenticationMethod()
		if err := cl.WritePacket(out); err != nil {
			return false, err
		}
		return out.ReasonCode == packets.CodeSuccess.Code, nil
	case 0:
		// Zero packet: the hook completes the exchange without a final AUTH.
		return true, nil
	default:
		s.Log.Error("enhanced auth hook returned a non-AUTH packet",
			"client", cl.ID, "packet_type", out.FixedHeader.Type)
		return false, packets.ErrImplementationSpecificError
	}
}

// completeHandshakeAuth transitions the connection from pending to
// authenticated and performs the one-time session establishment (session
// inheritance/takeover, client registration, CONNACK, inflight resend). The
// CAS guard ensures duplicate/concurrent AUTH packets can never advance the
// session twice. Note the wire ordering: for the initial handshake the hook's
// final AUTH(0x00) frame is written before this method runs, and CONNACK is
// written inside establishClientSession (so AUTH 0x00 precedes CONNACK per
// §4.12). The authenticated state is therefore guaranteed to be settled by
// the time CONNACK is sent, and no business packet is read before this method
// returns, so there is no observable window for gated traffic. (The
// re-authentication path additionally settles state before the AUTH 0x00
// frame is written; see invokeReAuthHook.)
func (s *Server) completeHandshakeAuth(cl *Client, connectPk packets.Packet) error {
	if !cl.casAuthState(AuthStatePending, AuthStateAuthenticated) {
		// Already settled (failure or duplicate completion); never establish twice.
		return packets.ErrProtocolViolation
	}

	atomic.AddInt64(&s.Info.EnhancedAuthPending, -1)
	atomic.AddInt64(&s.Info.EnhancedAuthSucceeded, 1)
	s.hooks.OnAuthStateChange(cl, AuthStateAuthenticated, cl.AuthenticationMethod(), packets.CodeSuccess)
	s.Log.Info("enhanced authentication succeeded",
		"client", cl.ID, "remote", cl.Net.Remote, "method", cl.AuthenticationMethod())

	if err := s.establishClientSession(cl, connectPk); err != nil {
		return err
	}

	return errEnhancedAuthCompleted
}

// failHandshakeAuth settles state and counters for a failed initial handshake
// and emits the terminal protocol response. CONNECT-stage failures are
// signalled with CONNACK (viaConnack=true); post-CONNECT structural violations
// are signalled with DISCONNECT.
func (s *Server) failHandshakeAuth(cl *Client, code packets.Code, viaConnack bool) error {
	if cl.casAuthState(AuthStatePending, AuthStateFailed) {
		atomic.AddInt64(&s.Info.EnhancedAuthPending, -1)
		atomic.AddInt64(&s.Info.EnhancedAuthFailed, 1)
		s.hooks.OnAuthStateChange(cl, AuthStateFailed, cl.AuthenticationMethod(), code)
	}

	s.Log.Warn("enhanced authentication failed",
		"client", cl.ID, "remote", cl.Net.Remote, "method", cl.AuthenticationMethod(),
		"reason", code.Reason, "code", code.Code)

	if viaConnack {
		if err := s.SendConnack(cl, code, false, nil); err != nil {
			return err
		}
	} else {
		_ = s.DisconnectClient(cl, code)
	}

	return code
}

// processAuth processes an inbound AUTH packet on an established connection
// (MQTT v5.0 §3.15 AUTH packet, §4.12.2 Re-authentication). On a connection
// outside the enhanced authentication lifecycle, any AUTH packet is a
// protocol violation.
func (s *Server) processAuth(cl *Client, pk packets.Packet) error {
	// AUTH is an MQTT v5 packet type; for v3 clients packet type 15 is
	// reserved, and plain v5 connections (no Authentication Method in
	// CONNECT) must never receive AUTH packets.
	if cl.Properties.ProtocolVersion < 5 || cl.AuthState() == AuthStateNone {
		return packets.ErrProtocolViolation
	}

	switch cl.AuthState() {
	case AuthStateAuthenticated:
		// Re-authentication is initiated by the client with AUTH 0x19
		// (MQTT v5.0 §4.12.1); any other reason code in this state is out of
		// phase.
		if pk.ReasonCode != packets.CodeReAuthenticate.Code {
			return packets.ErrProtocolViolation
		}
		if pk.Properties.AuthenticationMethod == "" || pk.Properties.AuthenticationMethod != cl.AuthenticationMethod() {
			// [MQTT-4.12.1-1] re-authentication must use the same
			// Authentication Method as the original authentication.
			return packets.ErrBadAuthenticationMethod
		}
		if !cl.casAuthState(AuthStateAuthenticated, AuthStateReAuthenticating) {
			return packets.ErrProtocolViolation // duplicate/concurrent re-auth
		}
		atomic.AddInt64(&s.Info.EnhancedAuthPending, 1)
		s.hooks.OnAuthStateChange(cl, AuthStateReAuthenticating, cl.AuthenticationMethod(), packets.CodeReAuthenticate)
		s.Log.Info("re-authentication started", "client", cl.ID, "method", cl.AuthenticationMethod())
		return s.invokeReAuthHook(cl, pk)

	case AuthStateReAuthenticating:
		// A second 0x19 while the exchange is in progress is a duplicate.
		if pk.ReasonCode == packets.CodeReAuthenticate.Code {
			return packets.ErrProtocolViolation
		}
		// Client->server continuation must be AUTH 0x18; 0x00 is server->client only.
		if pk.ReasonCode != packets.CodeContinueAuthentication.Code {
			return packets.ErrProtocolViolation
		}
		if pk.Properties.AuthenticationMethod == "" || pk.Properties.AuthenticationMethod != cl.AuthenticationMethod() {
			// [MQTT-4.12.1-1] re-authentication must use the same method.
			return packets.ErrBadAuthenticationMethod
		}
		return s.invokeReAuthHook(cl, pk)

	default:
		// Pending (handshake loop owns those packets) or Failed.
		return packets.ErrProtocolViolation
	}
}

// invokeReAuthHook invokes the authentication hook for a re-authentication
// step and applies the hook's decision. The established session is never
// torn down or re-established; on success only the auth state moves back to
// authenticated. The state transition (and its observation event) happens
// before the final AUTH(0x00) frame is written, so a client observing the
// success frame is guaranteed to see the authenticated state.
func (s *Server) invokeReAuthHook(cl *Client, pk packets.Packet) error {
	out, err := s.hooks.OnAuthPacket(cl, pk)
	if err != nil {
		return s.failReAuth(cl, authHookErrorCode(err))
	}

	switch out.FixedHeader.Type {
	case packets.Auth:
		if out.ReasonCode != packets.CodeContinueAuthentication.Code &&
			out.ReasonCode != packets.CodeSuccess.Code {
			s.Log.Error("enhanced auth hook returned an illegal AUTH reason code",
				"client", cl.ID, "reason_code", out.ReasonCode)
			return s.failReAuth(cl, packets.ErrImplementationSpecificError)
		}
		// [MQTT-4.12.0-5] server->client AUTH packets use the same
		// Authentication Method as the exchange.
		out.Properties.AuthenticationMethod = cl.AuthenticationMethod()

		if out.ReasonCode == packets.CodeSuccess.Code {
			// Settle the state first, then deliver the success frame.
			if !cl.casAuthState(AuthStateReAuthenticating, AuthStateAuthenticated) {
				return packets.ErrProtocolViolation // settled concurrently; never advance twice
			}
			atomic.AddInt64(&s.Info.EnhancedAuthPending, -1)
			atomic.AddInt64(&s.Info.EnhancedAuthSucceeded, 1)
			s.hooks.OnAuthStateChange(cl, AuthStateAuthenticated, cl.AuthenticationMethod(), packets.CodeSuccess)
			s.Log.Info("re-authentication succeeded", "client", cl.ID, "method", cl.AuthenticationMethod())

			if err := cl.WritePacket(out); err != nil {
				return err // connection is dead; teardown reconciles counters
			}
			return nil
		}

		// Continue challenge: write and wait for the next client AUTH.
		if err := cl.WritePacket(out); err != nil {
			return err
		}
		return nil
	case 0:
		// Zero packet: hook completes re-authentication without a final AUTH frame.
		if !cl.casAuthState(AuthStateReAuthenticating, AuthStateAuthenticated) {
			return packets.ErrProtocolViolation
		}
		atomic.AddInt64(&s.Info.EnhancedAuthPending, -1)
		atomic.AddInt64(&s.Info.EnhancedAuthSucceeded, 1)
		s.hooks.OnAuthStateChange(cl, AuthStateAuthenticated, cl.AuthenticationMethod(), packets.CodeSuccess)
		s.Log.Info("re-authentication succeeded", "client", cl.ID, "method", cl.AuthenticationMethod())
		return nil
	default:
		s.Log.Error("enhanced auth hook returned a non-AUTH packet",
			"client", cl.ID, "packet_type", out.FixedHeader.Type)
		return s.failReAuth(cl, packets.ErrImplementationSpecificError)
	}
}

// failReAuth settles state for a failed re-authentication and returns the
// reason code; receivePacket emits the DISCONNECT for v5 connections.
func (s *Server) failReAuth(cl *Client, code packets.Code) error {
	if cl.casAuthState(AuthStateReAuthenticating, AuthStateFailed) {
		atomic.AddInt64(&s.Info.EnhancedAuthPending, -1)
		atomic.AddInt64(&s.Info.EnhancedAuthFailed, 1)
		s.hooks.OnAuthStateChange(cl, AuthStateFailed, cl.AuthenticationMethod(), code)
	}

	s.Log.Warn("re-authentication failed",
		"client", cl.ID, "method", cl.AuthenticationMethod(), "reason", code.Reason, "code", code.Code)
	return code
}

// reconcileEnhancedAuthDisconnect settles enhanced authentication state and
// counters when a connection ends while an exchange was still in progress
// (client abandoned the handshake or the network dropped during
// re-authentication). It is idempotent: already-settled states are left
// untouched.
func (s *Server) reconcileEnhancedAuthDisconnect(cl *Client) {
	state := cl.AuthState()
	if state != AuthStatePending && state != AuthStateReAuthenticating {
		return
	}

	atomic.AddInt64(&s.Info.EnhancedAuthPending, -1)
	atomic.AddInt64(&s.Info.EnhancedAuthFailed, 1)
	cl.storeAuthState(AuthStateFailed)

	reason := packets.CodeDisconnect
	if cause := cl.StopCause(); cause != nil {
		if code, ok := cause.(packets.Code); ok {
			reason = code
		}
	}

	s.hooks.OnAuthStateChange(cl, AuthStateFailed, cl.AuthenticationMethod(), reason)
	s.Log.Warn("enhanced authentication interrupted by connection close",
		"client", cl.ID, "method", cl.AuthenticationMethod(), "state", state.String(), "cause", cl.StopCause())
}

// authHookErrorCode maps an error returned by an enhanced authentication hook
// to a deterministic MQTT reason code: packets.Code values are used as-is,
// any other error maps to Not Authorized (0x87).
func authHookErrorCode(err error) packets.Code {
	var code packets.Code
	if errors.As(err, &code) {
		return code
	}
	return packets.ErrNotAuthorized
}
