package bgp

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"testing"

	log "log/slog"

	"github.com/kube-vip/kube-vip/pkg/kubevip"
	"github.com/kube-vip/kube-vip/pkg/metrics"
	api "github.com/osrg/gobgp/v4/api"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	packetbgp "github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

func TestBGPSessionMetricTransitions(t *testing.T) {
	metrics.BGPSessionInfoGauge.Reset()
	t.Cleanup(metrics.BGPSessionInfoGauge.Reset)

	peer := apiutil.Peer{
		State: apiutil.PeerState{
			NeighborAddress: netip.MustParseAddr("192.0.2.30"),
			SessionState:    packetbgp.BGP_FSM_ESTABLISHED,
		},
	}
	updateSessionMetric(&apiutil.WatchEventMessage_PeerEvent{
		Type: apiutil.PEER_EVENT_STATE,
		Peer: peer,
	})

	const peerLabel = "192.0.2.30:179"
	assertSessionMetricState(t, peerLabel, "SESSION_STATE_ESTABLISHED", 1)
	assertSessionMetricState(t, peerLabel, "SESSION_STATE_IDLE", 0)

	// INIT and END_OF_INIT events do not carry a session state and must not
	// overwrite the most recent state gauge values.
	updateSessionMetric(&apiutil.WatchEventMessage_PeerEvent{Type: apiutil.PEER_EVENT_INIT})
	updateSessionMetric(&apiutil.WatchEventMessage_PeerEvent{Type: apiutil.PEER_EVENT_END_OF_INIT})
	assertSessionMetricState(t, peerLabel, "SESSION_STATE_ESTABLISHED", 1)

	peer.State.SessionState = packetbgp.BGP_FSM_IDLE
	updateSessionMetric(&apiutil.WatchEventMessage_PeerEvent{
		Type: apiutil.PEER_EVENT_STATE,
		Peer: peer,
	})
	assertSessionMetricState(t, peerLabel, "SESSION_STATE_IDLE", 1)
	assertSessionMetricState(t, peerLabel, "SESSION_STATE_ESTABLISHED", 0)
}

func updateSessionMetric(event *apiutil.WatchEventMessage_PeerEvent) {
	if event.Type != apiutil.PEER_EVENT_STATE {
		return
	}

	peerDescription := net.JoinHostPort(event.Peer.State.NeighborAddress.String(), "179")
	for stateName, stateValue := range api.PeerState_SessionState_value {
		metricValue := 0.0
		if int(event.Peer.State.SessionState) == int(stateValue)-1 {
			metricValue = 1
		}
		metrics.BGPSessionInfoGauge.With(prometheus.Labels{
			"state": stateName,
			"peer":  peerDescription,
		}).Set(metricValue)
	}
}

func assertSessionMetricState(t *testing.T, peer, state string, want float64) {
	t.Helper()
	if got := testutil.ToFloat64(metrics.BGPSessionInfoGauge.WithLabelValues(state, peer)); got != want {
		t.Errorf("BGP session metric %s/%s = %v, want %v", state, peer, got, want)
	}
}
