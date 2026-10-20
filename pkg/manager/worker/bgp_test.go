package worker

import (
	"net/netip"
	"testing"

	"github.com/kube-vip/kube-vip/pkg/metrics"
	"github.com/osrg/gobgp/v4/pkg/apiutil"
	packetbgp "github.com/osrg/gobgp/v4/pkg/packet/bgp"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const sessionMetricPeerLabel = "192.0.2.30:179"

func TestUpdateBGPSessionMetricTransitions(t *testing.T) {
	metrics.BGPSessionInfoGauge.Reset()
	t.Cleanup(metrics.BGPSessionInfoGauge.Reset)

	peer := apiutil.Peer{
		State: apiutil.PeerState{
			NeighborAddress: netip.MustParseAddr("192.0.2.30"),
			SessionState:    packetbgp.BGP_FSM_ESTABLISHED,
		},
	}
	updateBGPSessionMetric(&apiutil.WatchEventMessage_PeerEvent{
		Type: apiutil.PEER_EVENT_STATE,
		Peer: peer,
	})

	assertSessionMetricState(t, "SESSION_STATE_ESTABLISHED", 1)
	assertSessionMetricState(t, "SESSION_STATE_IDLE", 0)

	updateBGPSessionMetric(&apiutil.WatchEventMessage_PeerEvent{Type: apiutil.PEER_EVENT_INIT})
	updateBGPSessionMetric(&apiutil.WatchEventMessage_PeerEvent{Type: apiutil.PEER_EVENT_END_OF_INIT})
	assertSessionMetricState(t, "SESSION_STATE_ESTABLISHED", 1)

	peer.State.SessionState = packetbgp.BGP_FSM_IDLE
	updateBGPSessionMetric(&apiutil.WatchEventMessage_PeerEvent{
		Type: apiutil.PEER_EVENT_STATE,
		Peer: peer,
	})
	assertSessionMetricState(t, "SESSION_STATE_IDLE", 1)
	assertSessionMetricState(t, "SESSION_STATE_ESTABLISHED", 0)
}

func assertSessionMetricState(t *testing.T, state string, want float64) {
	t.Helper()
	if got := testutil.ToFloat64(metrics.BGPSessionInfoGauge.WithLabelValues(state, sessionMetricPeerLabel)); got != want {
		t.Errorf("BGP session metric %s/%s = %v, want %v", state, sessionMetricPeerLabel, got, want)
	}
}
