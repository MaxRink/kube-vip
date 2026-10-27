package bgp

import (
	"context"
	"strings"
	"testing"

	log "log/slog"

	"github.com/kube-vip/kube-vip/pkg/kubevip"
)

func TestNewBGPServerValidation(t *testing.T) {
	t.Parallel()

	peer := kubevip.BGPPeer{Address: "192.0.2.10", AS: 65001}
	tests := []struct {
		name   string
		config kubevip.BGPConfig
		want   string
	}{
		{name: "missing autonomous system", config: kubevip.BGPConfig{Peers: []kubevip.BGPPeer{peer}}, want: "provide AS"},
		{name: "conflicting source settings", config: kubevip.BGPConfig{AS: 65000, SourceIP: "192.0.2.1", SourceIF: "eth1", Peers: []kubevip.BGPPeer{peer}}, want: "mutually exclusive"},
		{name: "missing peers", config: kubevip.BGPConfig{AS: 65000}, want: "at least one peer"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := NewBGPServer(tt.config, log.LevelError)
			if err == nil {
				t.Fatalf("NewBGPServer() error = nil, want error containing %q; server = %v", tt.want, server)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("NewBGPServer() error = %q, want substring %q", err, tt.want)
			}
		})
	}

	server, err := NewBGPServer(kubevip.BGPConfig{
		AS:       65000,
		Peers:    []kubevip.BGPPeer{peer},
		RouterID: "192.0.2.2",
	}, log.LevelError)
	if err != nil {
		t.Fatalf("NewBGPServer() valid config error = %v", err)
	}
	if server.s == nil {
		t.Fatal("NewBGPServer() returned a nil embedded server")
	}
	if server.tracker == nil {
		t.Fatal("NewBGPServer() returned a nil route tracker")
	}
}

func TestAddPeerFallsBackToRegularBGPWhenMPBGPAddressCannotBeConfigured(t *testing.T) {
	server, err := NewBGPServer(kubevip.BGPConfig{
		AS:       65000,
		RouterID: "192.0.2.2",
		Peers:    []kubevip.BGPPeer{{Address: "192.0.2.10", AS: 65001}},
	}, log.LevelError)
	if err != nil {
		t.Fatalf("NewBGPServer() error = %v", err)
	}

	// The management loop is enough for AddPeer to return its final GoBGP error.
	// StartBgp is deliberately not called, so this test cannot create a BGP socket.
	go server.s.Serve()
	defer server.s.Stop()

	err = server.AddPeer(context.Background(), kubevip.BGPPeer{
		Address:      "192.0.2.10",
		AS:           65001,
		MpbgpNexthop: "fixed",
		MultiHop:     true,
		Password:     "secret",
	})
	if err == nil {
		t.Fatal("AddPeer() error = nil, want inactive-server error")
	}
	if strings.Contains(err.Error(), "failed to get MP-BGP addresses") {
		t.Fatalf("AddPeer() returned the MP-BGP setup error instead of falling back: %v", err)
	}
	if !strings.Contains(err.Error(), "hasn't started yet") {
		t.Fatalf("AddPeer() error = %q, want final regular-BGP error", err)
	}
}
