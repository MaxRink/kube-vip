package servicecontext

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestNewContextCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	ctx := New(parent)

	if ctx.Ctx == nil || ctx.Cancel == nil {
		t.Fatal("New returned an incomplete context")
	}
	if ctx.IsWatched {
		t.Fatal("new context should not be watched")
	}
	if ctx.Signalled.Load() {
		t.Fatal("new context should not be signalled")
	}

	ctx.Cancel()
	ctx.Cancel()
	cancelParent()
	if err := ctx.Ctx.Err(); err == nil {
		t.Fatal("Cancel did not cancel the service context")
	}

	parent, cancelParent = context.WithCancel(context.Background())
	ctx = New(parent)
	cancelParent()
	if err := ctx.Ctx.Err(); err == nil {
		t.Fatal("parent cancellation did not reach the service context")
	}
}

func TestConfiguredNetworks(t *testing.T) {
	ctx := New(context.Background())

	if ctx.HasConfiguredNetworks() {
		t.Fatal("new context unexpectedly has configured networks")
	}
	if ctx.IsNetworkConfigured("192.0.2.10") {
		t.Fatal("network unexpectedly reported as configured")
	}

	ctx.ConfiguredNetworks.Store("192.0.2.10", true)
	ctx.ConfiguredNetworks.Store("2001:db8::10", true)
	if !ctx.HasConfiguredNetworks() {
		t.Fatal("configured networks were not detected")
	}
	if !ctx.IsNetworkConfigured("192.0.2.10") || !ctx.IsNetworkConfigured("2001:db8::10") {
		t.Fatal("stored networks could not be read")
	}

	ctx.ConfiguredNetworks.Delete("192.0.2.10")
	if ctx.IsNetworkConfigured("192.0.2.10") {
		t.Fatal("deleted network was still reported")
	}
	if !ctx.HasConfiguredNetworks() {
		t.Fatal("remaining configured network was not detected")
	}

	ctx.ConfiguredNetworks.Clear()
	if ctx.HasConfiguredNetworks() {
		t.Fatal("cleared networks were still reported")
	}
	ctx.ConfiguredNetworks.Delete("missing")
}

func TestStartLeaderElectionOnceConcurrent(t *testing.T) {
	ctx := New(context.Background())
	var calls atomic.Int32
	var wg sync.WaitGroup

	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx.StartLeaderElectionOnce(func() {
				calls.Add(1)
			})
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("leader election callback called %d times, want 1", got)
	}
}

func TestReadinessSignalAndReset(t *testing.T) {
	ctx := New(context.Background())
	first := ctx.EndpointsReady

	ctx.SignalReadiness()
	ctx.SignalReadiness()
	select {
	case <-first:
	default:
		t.Fatal("SignalReadiness did not close the readiness channel")
	}
	if !ctx.Signalled.Load() {
		t.Fatal("SignalReadiness did not mark the context as signalled")
	}

	ctx.ResetReadiness()
	second := ctx.EndpointsReady
	if second == first {
		t.Fatal("ResetReadiness did not create a new readiness channel")
	}
	if ctx.Signalled.Load() {
		t.Fatal("ResetReadiness did not clear the signalled state")
	}
	select {
	case <-second:
		t.Fatal("reset readiness channel was already closed")
	default:
	}

	ctx.SignalReadiness()
	select {
	case <-second:
	default:
		t.Fatal("readiness was not signalled after reset")
	}

	ctx.ResetReadiness()
	ctx.ResetReadiness()
	if ctx.Signalled.Load() {
		t.Fatal("repeated ResetReadiness left the context signalled")
	}
}

func TestConcurrentNetworkAndCancellationAccess(t *testing.T) {
	ctx := New(context.Background())
	var wg sync.WaitGroup

	for worker := range 32 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := fmt.Sprintf("192.0.2.%d", worker+1)
			for iteration := range 200 {
				ctx.ConfiguredNetworks.Store(key, iteration)
				ctx.IsNetworkConfigured(key)
				ctx.HasConfiguredNetworks()
				if iteration%2 == 0 {
					ctx.ConfiguredNetworks.Delete(key)
				}
			}
		}(worker)
	}

	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx.Cancel()
		}()
	}
	wg.Wait()

	if ctx.Ctx.Err() == nil {
		t.Fatal("concurrent cancellation did not cancel the context")
	}
	ctx.ConfiguredNetworks.Clear()
	if ctx.HasConfiguredNetworks() {
		t.Fatal("network map was not empty after cleanup")
	}
}
