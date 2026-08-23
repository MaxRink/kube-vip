package cluster_test

import (
	"context"
	"io/fs"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kube-vip/kube-vip/pkg/arp"
	"github.com/kube-vip/kube-vip/pkg/cluster"
	"github.com/kube-vip/kube-vip/pkg/kubevip"
	"github.com/kube-vip/kube-vip/pkg/lease"
	"github.com/kube-vip/kube-vip/pkg/route"
	"github.com/kube-vip/kube-vip/pkg/vip"
)

type lifecycleNetwork struct {
	*mockNetwork

	addStarted  chan struct{}
	releaseAdd  chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
	operationMu sync.Mutex
	deleteCalls atomic.Int32
}

func newLifecycleNetwork() *lifecycleNetwork {
	return &lifecycleNetwork{
		mockNetwork: &mockNetwork{ip: "10.0.0.1", cidr: testCIDR},
		addStarted:  make(chan struct{}),
		releaseAdd:  make(chan struct{}),
	}
}

func (n *lifecycleNetwork) AddIP(bool, bool, ...int) (bool, error) {
	n.startOnce.Do(func() { close(n.addStarted) })

	n.operationMu.Lock()
	defer n.operationMu.Unlock()
	<-n.releaseAdd

	return n.mockNetwork.AddIP(false, false)
}

func (n *lifecycleNetwork) DeleteIP() (bool, error) {
	n.deleteCalls.Add(1)

	n.operationMu.Lock()
	defer n.operationMu.Unlock()
	return n.mockNetwork.DeleteIP()
}

func (n *lifecycleNetwork) release() {
	n.releaseOnce.Do(func() { close(n.releaseAdd) })
}

type deleteTrackingNetwork struct {
	*mockNetwork
	deleteCalls atomic.Int32
}

func (n *deleteTrackingNetwork) DeleteIP() (bool, error) {
	n.deleteCalls.Add(1)
	return n.mockNetwork.DeleteIP()
}

type routeReassertNetwork struct {
	*mockNetwork
	addRouteCalls     atomic.Int32
	replaceRouteCalls atomic.Int32
	routePresent      atomic.Bool
	missingOnce       atomic.Bool
}

func (n *routeReassertNetwork) AddRoute(bool) (bool, error) {
	n.addRouteCalls.Add(1)
	n.routePresent.Store(true)
	return true, nil
}

func (n *routeReassertNetwork) ReplaceRoute() error {
	n.replaceRouteCalls.Add(1)
	if n.missingOnce.CompareAndSwap(true, false) {
		return fs.ErrNotExist
	}
	n.routePresent.Store(true)
	return nil
}

func (n *routeReassertNetwork) removeRouteExternally() {
	n.routePresent.Store(false)
	n.missingOnce.Store(true)
}

func TestStartVipService_ConcurrentAddIPAndShutdownCompletes(t *testing.T) {
	cfg := &kubevip.Config{}
	c, err := cluster.InitCluster(cfg, true, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("InitCluster: %v", err)
	}

	network := newLifecycleNetwork()
	c.Network = []vip.Network{network}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.Cleanup(func() {
		cancel()
		network.release()
	})

	serviceDone := make(chan struct{})
	serviceErr := make(chan error, 1)
	go func() {
		serviceErr <- c.StartVipService(ctx, cfg, nil, nil, func() {})
		close(serviceDone)
	}()

	go func() {
		<-ctx.Done()
		network.release()
	}()

	select {
	case <-network.addStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("StartVipService did not reach AddIP")
	}

	testCtx, testCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer testCancel()

	shutdownStart := make(chan struct{})
	stopDone := make(chan struct{})
	leadershipDone := make(chan struct{})
	objLease := &lease.Lease{Ctx: ctx, Cancel: cancel}

	go func() {
		<-shutdownStart
		c.Stop()
		close(stopDone)
	}()
	go func() {
		<-shutdownStart
		c.OnStoppedLeading(cfg, objLease, nil)
		close(leadershipDone)
	}()
	close(shutdownStart)

	select {
	case <-stopDone:
	case <-testCtx.Done():
		t.Fatal("cluster Stop did not return during concurrent shutdown")
	}
	select {
	case <-leadershipDone:
	case <-testCtx.Done():
		t.Fatal("leadership shutdown deadlocked while AddIP was in progress")
	}
	select {
	case <-serviceDone:
		if err := <-serviceErr; err != nil {
			t.Fatalf("StartVipService returned an error: %v", err)
		}
	case <-testCtx.Done():
		t.Fatal("StartVipService did not exit within the 10-second deadlock guard")
	}

	if got := network.deleteCalls.Load(); got != 1 {
		t.Fatalf("DeleteIP calls = %d, want 1", got)
	}
}

func TestOnStoppedLeading_DeletesVIPWithoutPreservation(t *testing.T) {
	t.Parallel()

	cfg := &kubevip.Config{}
	c, err := cluster.InitCluster(cfg, true, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("InitCluster: %v", err)
	}

	network := &deleteTrackingNetwork{
		mockNetwork: &mockNetwork{ip: "10.0.0.1", cidr: testCIDR},
	}
	c.Network = []vip.Network{network}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	objLease := &lease.Lease{Ctx: ctx, Cancel: cancel}

	c.OnStoppedLeading(cfg, objLease, nil)

	if got := network.deleteCalls.Load(); got != 1 {
		t.Fatalf("DeleteIP calls = %d, want 1", got)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("leadership loss did not cancel the lease context")
	}
}

func TestOnStoppedLeading_PreservesVIPAndStopsAdvertisement(t *testing.T) {
	cfg := &kubevip.Config{
		EnableARP:                   true,
		PreserveVIPOnLeadershipLoss: true,
	}
	arpManager := arp.NewManager(cfg)
	c, err := cluster.InitCluster(cfg, true, nil, arpManager, nil, nil)
	if err != nil {
		t.Fatalf("InitCluster: %v", err)
	}

	network := &deleteTrackingNetwork{
		mockNetwork: &mockNetwork{ip: "10.0.0.1", cidr: testCIDR},
	}
	c.Network = []vip.Network{network}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	objLease := &lease.Lease{Ctx: ctx, Cancel: cancel}
	serviceDone := make(chan struct{})
	go func() {
		_ = c.StartVipService(ctx, cfg, nil, nil, func() {})
		close(serviceDone)
	}()

	expectEventually(t, func() bool {
		return arpManager.Count(network.ARPName()) == 1
	}, "ARP advertisement should start")

	c.OnStoppedLeading(cfg, objLease, nil)

	select {
	case <-serviceDone:
	case <-time.After(5 * time.Second):
		t.Fatal("VIP service did not stop after leadership loss")
	}
	if got := network.deleteCalls.Load(); got != 0 {
		t.Fatalf("DeleteIP calls = %d, want 0 when VIP preservation is enabled", got)
	}
	if !network.isPresent() {
		t.Fatal("VIP was removed from the interface despite preservation being enabled")
	}
	expectEventually(t, func() bool {
		return arpManager.Count(network.ARPName()) == 0
	}, "ARP advertisement should stop after leadership loss")
}

func TestRoutingTableHealthCheck_ReassertsExternallyRemovedRoute(t *testing.T) {
	healthcheck := newTestHealthServer(t, 200)
	t.Cleanup(healthcheck.server.Close)

	cfg := newRoutingTableConfig(healthcheck.server.URL, healthcheck.caPath)
	c, err := cluster.InitCluster(cfg, true, nil, nil, route.NewManager(), nil)
	if err != nil {
		t.Fatalf("InitCluster: %v", err)
	}

	network := &routeReassertNetwork{
		mockNetwork: &mockNetwork{ip: "10.0.0.1", cidr: testCIDR},
	}
	c.Network = []vip.Network{network}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serviceDone := make(chan struct{})
	go func() {
		_ = c.StartVipService(ctx, cfg, nil, nil, func() {})
		close(serviceDone)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-serviceDone:
		case <-time.After(5 * time.Second):
			t.Error("VIP service did not stop during cleanup")
		}
	})

	expectEventually(t, func() bool {
		return network.addRouteCalls.Load() >= 1
	}, "healthy check should add the route")

	network.removeRouteExternally()
	expectEventually(t, func() bool {
		return network.replaceRouteCalls.Load() >= 2 && network.routePresent.Load()
	}, "healthy checks should retry RouteReplace after a missing route")
}
