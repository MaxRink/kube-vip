package election

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kube-vip/kube-vip/pkg/kubevip"
	"github.com/kube-vip/kube-vip/pkg/lease"
	coordinationv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientgotesting "k8s.io/client-go/testing"
)

const (
	electionTestNamespace = "default"
	electionTestLeaseName = "kube-vip-election-test"
	electionTestIdentity  = "node-a"
)

type fakeKubernetesAPI struct {
	fake   *fake.Clientset
	client *kubernetes.Clientset
}

func newFakeKubernetesAPI(t *testing.T, objects ...runtime.Object) *fakeKubernetesAPI {
	t.Helper()

	fakeClient := fake.NewSimpleClientset(objects...)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveFakeKubernetesRequest(w, r, fakeClient)
	}))
	t.Cleanup(server.Close)

	client, err := kubernetes.NewForConfig(&rest.Config{
		Host:    server.URL,
		QPS:     100,
		Burst:   100,
		Timeout: 5 * time.Second,
		ContentConfig: rest.ContentConfig{
			ContentType:        "application/json",
			AcceptContentTypes: "application/json",
		},
	})
	if err != nil {
		t.Fatalf("creating fake-backed Kubernetes client: %v", err)
	}

	return &fakeKubernetesAPI{fake: fakeClient, client: client}
}

func serveFakeKubernetesRequest(w http.ResponseWriter, r *http.Request, client *fake.Clientset) {
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 6 || parts[0] != "apis" || parts[1] != "coordination.k8s.io" || parts[2] != "v1" || parts[3] != "namespaces" || parts[5] != "leases" {
		http.Error(w, fmt.Sprintf("fake API: unhandled path %s %s", r.Method, r.URL.Path), http.StatusNotFound)
		return
	}

	namespace := parts[4]
	leases := client.CoordinationV1().Leases(namespace)

	var (
		object *coordinationv1.Lease
		err    error
	)

	switch {
	case r.Method == http.MethodGet && len(parts) == 7:
		object, err = leases.Get(r.Context(), parts[6], metav1.GetOptions{})
	case r.Method == http.MethodPost && len(parts) == 6:
		object = &coordinationv1.Lease{}
		err = json.NewDecoder(r.Body).Decode(object)
		if err == nil {
			object, err = leases.Create(r.Context(), object, metav1.CreateOptions{})
		}
	case r.Method == http.MethodPut && len(parts) == 7:
		object = &coordinationv1.Lease{}
		err = json.NewDecoder(r.Body).Decode(object)
		if err == nil {
			object, err = leases.Update(r.Context(), object, metav1.UpdateOptions{})
		}
	default:
		http.Error(w, fmt.Sprintf("fake API: unhandled verb %s %s", r.Method, r.URL.Path), http.StatusNotFound)
		return
	}

	if err != nil {
		writeFakeAPIError(w, err)
		return
	}
	writeFakeJSON(w, http.StatusOK, object)
}

func writeFakeJSON(w http.ResponseWriter, status int, object any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(object)
}

func writeFakeAPIError(w http.ResponseWriter, err error) {
	if statusErr, ok := err.(apierrors.APIStatus); ok {
		status := statusErr.Status()
		writeFakeJSON(w, int(status.Code), status)
		return
	}

	writeFakeJSON(w, http.StatusInternalServerError, &metav1.Status{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
		Status:   metav1.StatusFailure,
		Message:  err.Error(),
		Reason:   metav1.StatusReasonInternalError,
		Code:     http.StatusInternalServerError,
	})
}

func newElectionTestRun(client *kubernetes.Clientset, identity, leaseName string, started, stopped chan struct{}) (*RunConfig, *atomic.Value) {
	config := &kubevip.Config{
		LeaderElectionType: "kubernetes",
		NodeName:           identity,
		KubernetesLeaderElection: kubevip.KubernetesLeaderElection{
			LeaseDuration: 3,
			RenewDeadline: 2,
			RetryPeriod:   1,
		},
	}

	observedLeader := &atomic.Value{}
	run := &RunConfig{
		Config:  config,
		LeaseID: lease.NewID("kubernetes", electionTestNamespace, leaseName),
		Mgr: &Manager{
			KubernetesClient:   client,
			RetryWatcherClient: client,
		},
		OnStartedLeading: func(context.Context) {
			started <- struct{}{}
		},
		OnStoppedLeading: func() {
			stopped <- struct{}{}
		},
		OnNewLeader: func(leader string) {
			observedLeader.Store(leader)
		},
	}

	return run, observedLeader
}

func startElection(ctx context.Context, run *RunConfig) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		runKubernetesLeaderElectionOrDie(ctx, run)
		close(done)
	}()
	return done
}

func getTestLease(t *testing.T, client *fake.Clientset, name string) *coordinationv1.Lease {
	t.Helper()

	object, err := client.CoordinationV1().Leases(electionTestNamespace).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("getting lease %q: %v", name, err)
	}
	return object
}

func waitForSignal(t *testing.T, signal <-chan struct{}, description string) {
	t.Helper()

	select {
	case <-signal:
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func TestRunKubernetesLeaderElectionAcquiresLease(t *testing.T) {
	t.Parallel()

	api := newFakeKubernetesAPI(t)
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	run, observedLeader := newElectionTestRun(api.client, electionTestIdentity, electionTestLeaseName, started, stopped)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startElection(ctx, run)

	waitForSignal(t, started, "leader election to start leading")
	object := getTestLease(t, api.fake, electionTestLeaseName)
	if object.Spec.HolderIdentity == nil || *object.Spec.HolderIdentity != electionTestIdentity {
		t.Fatalf("lease holder = %v, want %q", object.Spec.HolderIdentity, electionTestIdentity)
	}
	if leader, _ := observedLeader.Load().(string); leader != electionTestIdentity {
		t.Fatalf("OnNewLeader observed %q, want %q", leader, electionTestIdentity)
	}

	cancel()
	waitForSignal(t, stopped, "leader election to stop")
	waitForSignal(t, done, "leader election goroutine to exit")
}

func TestRunKubernetesLeaderElectionReleasesLeaseOnCancel(t *testing.T) {
	t.Parallel()

	api := newFakeKubernetesAPI(t)
	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	run, _ := newElectionTestRun(api.client, electionTestIdentity, electionTestLeaseName, started, stopped)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startElection(ctx, run)

	waitForSignal(t, started, "leader election to start leading")
	cancel()
	waitForSignal(t, stopped, "leader election to stop after cancellation")
	waitForSignal(t, done, "leader election goroutine to exit")

	object := getTestLease(t, api.fake, electionTestLeaseName)
	if object.Spec.HolderIdentity == nil || *object.Spec.HolderIdentity != "" {
		t.Fatalf("released lease holder = %v, want an empty identity", object.Spec.HolderIdentity)
	}
}

func TestRunKubernetesLeaderElectionReacquiresAfterLeaseLoss(t *testing.T) {
	api := newFakeKubernetesAPI(t)
	const otherIdentity = "node-b"

	var lossActive atomic.Bool
	api.fake.PrependReactor("update", "leases", func(action clientgotesting.Action) (bool, runtime.Object, error) {
		update, ok := action.(clientgotesting.UpdateAction)
		if !ok || !lossActive.Load() {
			return false, nil, nil
		}

		// Calling back into the fake clientset from inside a reactor deadlocks on the
		// tracker lock, so decide from the incoming object alone: while lossActive is
		// set the lease is held by otherIdentity by construction, and any update that
		// does not preserve that holder must fail with a conflict.
		incoming := update.GetObject().(*coordinationv1.Lease)
		incomingHolder := ""
		if incoming.Spec.HolderIdentity != nil {
			incomingHolder = *incoming.Spec.HolderIdentity
		}
		if incomingHolder != otherIdentity {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: coordinationv1.GroupName, Resource: "leases"}, electionTestLeaseName, fmt.Errorf("lease was taken by %s", otherIdentity))
		}
		return false, nil, nil
	})

	started := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	run, _ := newElectionTestRun(api.client, electionTestIdentity, electionTestLeaseName, started, stopped)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := startElection(ctx, run)

	waitForSignal(t, started, "initial leader election to start leading")

	object := getTestLease(t, api.fake, electionTestLeaseName)
	otherHolder := otherIdentity
	object.Spec.HolderIdentity = &otherHolder
	now := metav1.NewMicroTime(time.Now())
	object.Spec.RenewTime = &now
	if _, err := api.fake.CoordinationV1().Leases(electionTestNamespace).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("stealing lease: %v", err)
	}
	lossActive.Store(true)

	waitForSignal(t, stopped, "leader election to report lease loss")
	waitForSignal(t, done, "leader election goroutine to exit after lease loss")

	// runKubernetesLeaderElectionOrDie exits after a loss. The caller owns the restart loop,
	// so free the lease and invoke it again to verify that a fresh run can be elected.
	lossActive.Store(false)
	object = getTestLease(t, api.fake, electionTestLeaseName)
	object.Spec.HolderIdentity = new(string)
	if _, err := api.fake.CoordinationV1().Leases(electionTestNamespace).Update(context.Background(), object, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("freeing lease for restart: %v", err)
	}

	startedAgain := make(chan struct{}, 1)
	stoppedAgain := make(chan struct{}, 1)
	runAgain, _ := newElectionTestRun(api.client, electionTestIdentity, electionTestLeaseName, startedAgain, stoppedAgain)
	ctxAgain, cancelAgain := context.WithCancel(context.Background())
	defer cancelAgain()
	doneAgain := startElection(ctxAgain, runAgain)

	waitForSignal(t, startedAgain, "restarted leader election to start leading")
	object = getTestLease(t, api.fake, electionTestLeaseName)
	if object.Spec.HolderIdentity == nil || *object.Spec.HolderIdentity != electionTestIdentity {
		t.Fatalf("restarted lease holder = %v, want %q", object.Spec.HolderIdentity, electionTestIdentity)
	}

	cancelAgain()
	waitForSignal(t, stoppedAgain, "restarted leader election to stop")
	waitForSignal(t, doneAgain, "restarted leader election goroutine to exit")
}

func TestCheckIfNodeIsReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		node      *v1.Node
		wantReady bool
	}{
		{name: "nil node", node: nil},
		{name: "missing condition", node: &v1.Node{}},
		{name: "not ready", node: &v1.Node{Status: v1.NodeStatus{Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionFalse}}}}},
		{name: "ready", node: &v1.Node{Status: v1.NodeStatus{Conditions: []v1.NodeCondition{{Type: v1.NodeReady, Status: v1.ConditionTrue}}}}, wantReady: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := checkIfNodeIsReady(test.node); got != test.wantReady {
				t.Fatalf("checkIfNodeIsReady() = %v, want %v", got, test.wantReady)
			}
		})
	}
}
