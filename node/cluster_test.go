package node_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-raftkit/raftkit/node"
	"github.com/hashicorp/raft"
)

// sigFSM records applied commands and signals each one on ch, so tests wait
// for replication on an event rather than polling.
type sigFSM struct {
	mu   sync.Mutex
	cmds []string
	ch   chan string
}

func newSigFSM() *sigFSM { return &sigFSM{ch: make(chan string, 16)} }

func (f *sigFSM) Apply(l *raft.Log) any {
	f.mu.Lock()
	f.cmds = append(f.cmds, string(l.Data))
	f.mu.Unlock()
	f.ch <- string(l.Data)
	return nil
}
func (f *sigFSM) Snapshot() (raft.FSMSnapshot, error) { return nopSnap{}, nil }
func (f *sigFSM) Restore(rc io.ReadCloser) error      { return rc.Close() }

func (f *sigFSM) waitFor(t *testing.T, want string) {
	t.Helper()
	select {
	case got := <-f.ch:
		if got != want {
			t.Fatalf("applied %q, want %q", got, want)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("%q not applied within 15s", want)
	}
}

// selfSigned returns a cert valid for 127.0.0.1 as server and client, usable
// as its own CA. Generated in memory; nothing is written to disk.
func selfSigned(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "raftkit-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

func mutualTLS(t *testing.T) *tls.Config {
	t.Helper()
	c := selfSigned(t)
	pool := x509.NewCertPool()
	pool.AddCert(c.Leaf)
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{c},
		RootCAs:      pool,
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
}

// replicate bootstraps node 0, adds two voters over the real network, applies
// a command on the leader and asserts all three FSMs converge.
func replicate(t *testing.T, durable bool, tlsCfg *tls.Config) {
	t.Helper()
	const size = 3
	fsms := make([]*sigFSM, size)
	nodes := make([]*node.Node, size)
	notify := make(chan bool, 4)
	for i := range nodes {
		fsms[i] = newSigFSM()
		rc := raft.DefaultConfig()
		if i == 0 {
			rc.NotifyCh = notify
		}
		cfg := node.Config{
			ID:        string(rune('a' + i)),
			FSM:       fsms[i],
			BindAddr:  "127.0.0.1:0",
			TLS:       tlsCfg,
			Bootstrap: i == 0,
			Raft:      rc,
		}
		if durable {
			cfg.DataDir = filepath.Join(t.TempDir(), "raft")
		} else {
			cfg.InMemory = true
		}
		n, err := node.New(cfg)
		if err != nil {
			t.Fatalf("node %d: %v", i, err)
		}
		defer n.Shutdown()
		nodes[i] = n
	}

	select {
	case leader := <-notify:
		if !leader {
			t.Fatal("notified of leadership loss, want gain")
		}
	case <-time.After(15 * time.Second):
		t.Fatal("no leadership within 15s")
	}

	for i := 1; i < size; i++ {
		f := nodes[0].Raft.AddVoter(raft.ServerID(string(rune('a'+i))), nodes[i].Transport.LocalAddr(), 0, 10*time.Second)
		if err := f.Error(); err != nil {
			t.Fatalf("AddVoter %d: %v", i, err)
		}
	}
	if _, err := nodes[0].Apply([]byte("replicated"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for i, f := range fsms {
		f.waitFor(t, "replicated")
		f.mu.Lock()
		if len(f.cmds) != 1 || f.cmds[0] != "replicated" {
			t.Errorf("fsm %d applied %v", i, f.cmds)
		}
		f.mu.Unlock()
	}
}

func TestThreeNodeReplicationTCP(t *testing.T)        { replicate(t, false, nil) }
func TestThreeNodeReplicationTCPDurable(t *testing.T) { replicate(t, true, nil) }
func TestThreeNodeReplicationTLS(t *testing.T)        { replicate(t, false, mutualTLS(t)) }
func TestThreeNodeReplicationTLSDurable(t *testing.T) { replicate(t, true, mutualTLS(t)) }

// TestDurableRestart proves the bbolt log survives a restart: the second
// incarnation replays the committed command into a fresh FSM.
func TestDurableRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "raft")
	start := func() (*node.Node, *sigFSM, chan bool) {
		fsm := newSigFSM()
		notify := make(chan bool, 4)
		rc := raft.DefaultConfig()
		rc.NotifyCh = notify
		n, err := node.New(node.Config{ID: "n1", FSM: fsm, DataDir: dir, Bootstrap: true, Raft: rc})
		if err != nil {
			t.Fatal(err)
		}
		return n, fsm, notify
	}
	n, _, notify := start()
	<-notify
	if _, err := n.Apply([]byte("persisted"), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.Shutdown(); err != nil {
		t.Fatal(err)
	}

	n2, fsm2, _ := start() // Bootstrap=true on existing state must be a no-op
	defer n2.Shutdown()
	fsm2.waitFor(t, "persisted")
}

func TestNewValidationBackends(t *testing.T) {
	for name, c := range map[string]node.Config{
		"both backends": {ID: "a", FSM: newSigFSM(), InMemory: true, DataDir: t.TempDir()},
		"tls no bind":   {ID: "a", FSM: newSigFSM(), InMemory: true, TLS: &tls.Config{}},
	} {
		if _, err := node.New(c); err == nil {
			t.Errorf("%s: want error", name)
		}
	}
}

// TestNewFailures pins every refusal on the assembly path: each must surface
// as an error, not a half-built node.
func TestNewFailures(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	busy := occupied.Addr().String()

	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	boltIsDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(boltIsDir, "raft.db"), 0o700); err != nil {
		t.Fatal(err)
	}
	snapIsFile := t.TempDir()
	if err := os.WriteFile(filepath.Join(snapIsFile, "snapshots"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	bad := raft.DefaultConfig()
	bad.HeartbeatTimeout = time.Nanosecond // fails raft.ValidateConfig

	for _, tc := range []struct {
		name, want string
		cfg        node.Config
	}{
		{"data dir is a file", "data dir", node.Config{DataDir: filepath.Join(file, "x")}},
		{"raft.db is a directory", "bolt store", node.Config{DataDir: boltIsDir}},
		{"snapshots is a file", "snapshot store", node.Config{DataDir: snapIsFile}},
		{"bind address busy", "tcp transport", node.Config{InMemory: true, BindAddr: busy}},
		{"tls bind address busy", "tls listen", node.Config{InMemory: true, BindAddr: busy, TLS: mutualTLS(t)}},
		{"tls unadvertisable bind", "not advertisable", node.Config{InMemory: true, BindAddr: "0.0.0.0:0", TLS: mutualTLS(t)}},
		{"bad advertise address", "advertise address", node.Config{InMemory: true, BindAddr: "127.0.0.1:0", Address: "no port"}},
		{"bootstrap bad config", "bootstrap", node.Config{InMemory: true, Bootstrap: true, Raft: bad}},
		{"new raft bad config", "new raft", node.Config{InMemory: true, Raft: bad}},
	} {
		c := tc.cfg
		c.ID, c.FSM = "a", newSigFSM()
		n, err := node.New(c)
		if err == nil {
			t.Errorf("%s: want error", tc.name)
			continue // a half-built node must not be shut down
		}
		if n != nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: got (%v, %v), want nil node and %q", tc.name, n, err, tc.want)
		}
	}
}

// TestTLSAdvertise checks that Address overrides the bound address.
func TestTLSAdvertise(t *testing.T) {
	n, err := node.New(node.Config{ID: "a", FSM: newSigFSM(), InMemory: true,
		BindAddr: "127.0.0.1:0", Address: "127.0.0.1:1", TLS: mutualTLS(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer n.Shutdown()
	if got := n.Transport.LocalAddr(); got != "127.0.0.1:1" {
		t.Fatalf("LocalAddr = %q, want 127.0.0.1:1", got)
	}
}
