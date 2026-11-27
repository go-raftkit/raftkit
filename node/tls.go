package node

import (
	"crypto/tls"
	"net"
	"time"

	"github.com/hashicorp/raft"
)

// tlsStreamLayer is a raft.StreamLayer over crypto/tls: Accept returns
// server-side TLS conns, Dial returns client-side ones.
type tlsStreamLayer struct {
	net.Listener // a tls.NewListener over TCP
	advertise    net.Addr
	cfg          *tls.Config
}

// Addr returns the advertise address if set, else the listener's.
func (s *tlsStreamLayer) Addr() net.Addr {
	if s.advertise != nil {
		return s.advertise
	}
	return s.Listener.Addr()
}

// Dial opens a TLS connection; the handshake completes before returning so a
// certificate failure surfaces here and not on the first write.
func (s *tlsStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: timeout}, Config: s.cfg}
	return d.Dial("tcp", string(address))
}
