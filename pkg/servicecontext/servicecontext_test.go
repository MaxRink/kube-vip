package servicecontext

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func resetReadiness(ctx *Context) bool {
	return ctx.ResetReadinessGeneration(ctx.CurrentReadiness())
}

func TestReadinessResetCreatesNewGeneration(t *testing.T) {
	svcCtx := New(context.Background())
	defer svcCtx.Cancel()

	firstGeneration := svcCtx.CurrentReadiness()
	if firstGeneration.ID() != 1 || svcCtx.IsReady() {
		t.Fatalf("initial readiness state = generation %d, ready %t; want generation 1, ready false", firstGeneration.ID(), svcCtx.IsReady())
	}
	svcCtx.SignalReadiness()
	select {
	case <-firstGeneration.Ready():
	default:
		t.Fatal("first readiness generation was not signalled")
	}

	if !resetReadiness(svcCtx) {
		t.Fatal("first readiness generation was not reset")
	}
	select {
	case <-firstGeneration.Lost():
	default:
		t.Fatal("first readiness generation loss was not signalled")
	}

	secondGeneration := svcCtx.CurrentReadiness()
	if secondGeneration.ID() != firstGeneration.ID()+1 || svcCtx.IsReady() {
		t.Fatalf("reset readiness state = generation %d, ready %t; want generation %d, ready false", secondGeneration.ID(), svcCtx.IsReady(), firstGeneration.ID()+1)
	}
	if firstGeneration.Ready() == secondGeneration.Ready() {
		t.Fatal("readiness reset reused the previous generation")
	}
	if firstGeneration.Lost() == secondGeneration.Lost() {
		t.Fatal("readiness reset reused the previous loss signal")
	}
	select {
	case <-secondGeneration.Ready():
		t.Fatal("new readiness generation was already signalled")
	default:
	}

	svcCtx.SignalReadiness()
	select {
	case <-secondGeneration.Ready():
	default:
		t.Fatal("second readiness generation was not signalled")
	}
}

func TestParentSurvivesServiceCancellation(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	svcCtx := New(parent)
	svcCtx.Cancel()
	if parent.Err() != nil {
		t.Fatal("cancelling a service context cancelled its parent")
	}
}

func TestResetReadinessGenerationWaitsForActivation(t *testing.T) {
	svcCtx := New(context.Background())
	defer svcCtx.Cancel()
	svcCtx.SignalReadiness()
	generation := svcCtx.CurrentReadiness()
	reservation, acquired := svcCtx.WaitForReadiness()
	if !acquired {
		t.Fatal("ready generation was not available for activation")
	}

	resetComplete := make(chan bool, 1)
	go func() {
		resetComplete <- svcCtx.ResetReadinessGeneration(generation)
	}()

	deadline := time.Now().Add(time.Second)
	for {
		currentGeneration := svcCtx.CurrentReadiness()
		if currentGeneration != generation {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("readiness reset did not start")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-resetComplete:
		t.Fatal("readiness reset returned before the activation released its generation")
	default:
	}

	reservation.Release()
	select {
	case reset := <-resetComplete:
		if !reset {
			t.Fatal("readiness reset rejected its current generation")
		}
	case <-time.After(time.Second):
		t.Fatal("readiness reset did not complete after activation released its generation")
	}
}

func TestWaitForReadinessReturnsAfterReadyContextIsCancelled(t *testing.T) {
	svcCtx := New(context.Background())
	svcCtx.SignalReadiness()
	svcCtx.Cancel()

	done := make(chan bool, 1)
	go func() {
		_, acquired := svcCtx.WaitForReadiness()
		done <- acquired
	}()

	select {
	case acquired := <-done:
		if acquired {
			t.Fatal("cancelled Service context returned a readiness reservation")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForReadiness spun after a ready Service context was cancelled")
	}
}

func TestStartWatchingClaimsOnce(t *testing.T) {
	svcCtx := New(context.Background())
	defer svcCtx.Cancel()

	var claims atomic.Int64
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if svcCtx.StartWatching() {
				claims.Add(1)
			}
		})
	}
	wg.Wait()

	if got := claims.Load(); got != 1 {
		t.Fatalf("watcher claims = %d, want 1", got)
	}
	svcCtx.StopWatching()
	if !svcCtx.StartWatching() {
		t.Fatal("watcher ownership was not released")
	}
}

func TestCancelledContextCannotStartWatching(t *testing.T) {
	svcCtx := New(context.Background())
	svcCtx.Cancel()
	if svcCtx.StartWatching() {
		t.Fatal("cancelled Service context acquired watcher ownership")
	}
}

func TestWaitForWatchingStopped(t *testing.T) {
	svcCtx := New(context.Background())
	defer svcCtx.Cancel()
	if !svcCtx.StartWatching() {
		t.Fatal("watcher ownership was not acquired")
	}

	done := make(chan error, 1)
	go func() {
		done <- svcCtx.WaitForWatchingStopped(context.Background())
	}()
	select {
	case <-done:
		t.Fatal("WaitForWatchingStopped returned while the watcher was active")
	case <-time.After(20 * time.Millisecond):
	}

	svcCtx.StopWatching()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("WaitForWatchingStopped() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForWatchingStopped did not return after watcher shutdown")
	}
}

func TestConcurrentStateTransitions(t *testing.T) {
	svcCtx := New(context.Background())
	defer svcCtx.Cancel()

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			svcCtx.SignalReadiness()
			resetReadiness(svcCtx)
		})
	}
	wg.Wait()
	if svcCtx.IsReady() && !resetReadiness(svcCtx) {
		t.Fatal("final readiness generation was not reset")
	}

	generation := svcCtx.CurrentReadiness()
	if svcCtx.IsReady() {
		t.Fatal("concurrent transitions left the context ready after every signal was reset")
	}
	if generation.ID() == 1 {
		t.Fatal("concurrent transitions did not advance the readiness generation")
	}
	select {
	case <-generation.Ready():
		t.Fatal("current readiness channel was already closed")
	default:
	}
	select {
	case <-generation.Lost():
		t.Fatal("current readiness-loss channel was already closed")
	default:
	}
}


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
