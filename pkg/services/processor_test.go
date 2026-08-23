package services

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kube-vip/kube-vip/pkg/instance"
	"github.com/kube-vip/kube-vip/pkg/kubevip"
	"github.com/kube-vip/kube-vip/pkg/lease"
	"github.com/kube-vip/kube-vip/pkg/servicecontext"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
)

func newTrackedProcessor(t *testing.T, service *v1.Service) (*Processor, *servicecontext.Context) {
	t.Helper()

	config := &kubevip.Config{}
	ctx := servicecontext.New(context.Background())
	ctx.IsWatched = true
	p := &Processor{
		config: config,
		lbClassFilter: func(*v1.Service, *kubevip.Config) bool {
			return false
		},
		leaseMgr: lease.NewManager(),
		ServiceInstances: []*instance.Instance{{
			ServiceSnapshot: service.DeepCopy(),
			AddCalled:       true,
		}},
	}
	p.svcMap.Store(service.UID, ctx)
	return p, ctx
}

func loadBalancerService(policy v1.ServiceExternalTrafficPolicyType, forceElection bool) *v1.Service {
	annotations := map[string]string{}
	if forceElection {
		annotations[kubevip.ForcePerServiceElection] = "true"
	}
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "example",
			Namespace:   "default",
			UID:         "service-uid",
			Annotations: annotations,
		},
		Spec: v1.ServiceSpec{
			Type:                  v1.ServiceTypeLoadBalancer,
			LoadBalancerIP:        "192.0.2.10",
			ExternalTrafficPolicy: policy,
		},
	}
}

func TestAddOrModifyThenDeleteIsIdempotentAcrossElectionModes(t *testing.T) {
	tests := []struct {
		name       string
		forcedOnly bool
		force      bool
		policy     v1.ServiceExternalTrafficPolicyType
		selected   bool
	}{
		{name: "global cluster", policy: v1.ServiceExternalTrafficPolicyTypeCluster, selected: true},
		{name: "global local", policy: v1.ServiceExternalTrafficPolicyTypeLocal, selected: true},
		{name: "per service cluster", forcedOnly: true, force: true, policy: v1.ServiceExternalTrafficPolicyTypeCluster, selected: true},
		{name: "per service local", forcedOnly: true, force: true, policy: v1.ServiceExternalTrafficPolicyTypeLocal, selected: true},
		{name: "global skips forced service", force: true, policy: v1.ServiceExternalTrafficPolicyTypeCluster},
		{name: "per service skips ordinary service", forcedOnly: true, policy: v1.ServiceExternalTrafficPolicyTypeCluster},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service := loadBalancerService(test.policy, test.force)
			p, svcCtx := newTrackedProcessor(t, service)
			defer svcCtx.Cancel()

			event := watch.Event{Type: watch.Added, Object: service}
			if err := p.AddOrModify(context.Background(), event, nil, test.forcedOnly, nil, nil); err != nil {
				t.Fatalf("AddOrModify returned error: %v", err)
			}

			if !test.selected {
				if _, ok := p.svcMap.Load(service.UID); !ok || len(p.ServiceInstances) != 1 {
					t.Fatal("an event for the other election mode changed the tracked service")
				}
				if err := p.Delete(watch.Event{Type: watch.Deleted, Object: service}, test.forcedOnly); err != nil {
					t.Fatalf("filtered Delete returned error: %v", err)
				}
				return
			}

			if err := p.Delete(watch.Event{Type: watch.Deleted, Object: service}, test.forcedOnly); err != nil {
				t.Fatalf("first Delete returned error: %v", err)
			}
			if svcCtx.Ctx.Err() == nil {
				t.Fatal("Delete did not cancel the service context")
			}
			if _, ok := p.svcMap.Load(service.UID); ok {
				t.Fatal("Delete left the service context tracked")
			}
			if len(p.ServiceInstances) != 0 {
				t.Fatalf("tracked service instance count = %d, want 0", len(p.ServiceInstances))
			}

			if err := p.Delete(watch.Event{Type: watch.Deleted, Object: service}, test.forcedOnly); err != nil {
				t.Fatalf("second Delete returned error: %v", err)
			}
			if len(p.ServiceInstances) != 0 {
				t.Fatal("second Delete changed the already empty service list")
			}
		})
	}
}

func TestCommonLeaseRejectsLocalTrafficPolicy(t *testing.T) {
	for _, policy := range []v1.ServiceExternalTrafficPolicyType{
		v1.ServiceExternalTrafficPolicyTypeCluster,
		v1.ServiceExternalTrafficPolicyTypeLocal,
	} {
		t.Run(string(policy), func(t *testing.T) {
			service := loadBalancerService(policy, false)
			service.Annotations[kubevip.ServiceLease] = "shared-lease"
			p, svcCtx := newTrackedProcessor(t, service)
			defer svcCtx.Cancel()

			err := p.AddOrModify(context.Background(), watch.Event{Type: watch.Added, Object: service}, nil, false, nil, nil)
			if policy == v1.ServiceExternalTrafficPolicyTypeLocal {
				if err == nil {
					t.Fatal("local service using a common lease was accepted")
				}
				return
			}
			if err != nil {
				t.Fatalf("cluster service using a common lease returned error: %v", err)
			}
		})
	}
}

type fakeServiceTransport struct {
	client *fake.Clientset
}

func newFakeBackedClient(t *testing.T, service *v1.Service) (*kubernetes.Clientset, *fake.Clientset) {
	t.Helper()

	fakeClient := fake.NewSimpleClientset(service)
	scheme := runtime.NewScheme()
	if err := v1.AddToScheme(scheme); err != nil {
		t.Fatalf("add core scheme: %v", err)
	}
	config := &rest.Config{
		Host:    "https://fake.invalid",
		APIPath: "/api",
		ContentConfig: rest.ContentConfig{
			AcceptContentTypes:   runtime.ContentTypeJSON,
			ContentType:          runtime.ContentTypeJSON,
			GroupVersion:         &schema.GroupVersion{Version: "v1"},
			NegotiatedSerializer: serializer.NewCodecFactory(scheme).WithoutConversion(),
		},
		Transport: &fakeServiceTransport{client: fakeClient},
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("create typed clientset: %v", err)
	}
	return client, fakeClient
}

func (t *fakeServiceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	if len(parts) < 6 || parts[0] != "api" || parts[1] != "v1" || parts[2] != "namespaces" || parts[4] != "services" {
		return fakeServiceResponse(req, nil, fmt.Errorf("unsupported path %q", req.URL.Path)), nil
	}
	namespace, name := parts[3], parts[5]
	subresource := ""
	if len(parts) == 7 {
		subresource = parts[6]
		if subresource != "status" {
			return fakeServiceResponse(req, nil, fmt.Errorf("unsupported subresource %q", subresource)), nil
		}
	} else if len(parts) != 6 {
		return fakeServiceResponse(req, nil, fmt.Errorf("unsupported path %q", req.URL.Path)), nil
	}
	var object runtime.Object
	var err error
	switch req.Method {
	case http.MethodGet:
		object, err = t.client.CoreV1().Services(namespace).Get(req.Context(), name, metav1.GetOptions{})
	case http.MethodPut:
		var service v1.Service
		if decodeErr := json.NewDecoder(req.Body).Decode(&service); decodeErr != nil {
			return fakeServiceResponse(req, nil, decodeErr), nil
		}
		if subresource == "status" {
			object, err = t.client.CoreV1().Services(namespace).UpdateStatus(req.Context(), &service, metav1.UpdateOptions{})
		} else {
			object, err = t.client.CoreV1().Services(namespace).Update(req.Context(), &service, metav1.UpdateOptions{})
		}
	default:
		err = fmt.Errorf("unsupported method %s", req.Method)
	}
	return fakeServiceResponse(req, object, err), nil
}

func fakeServiceResponse(req *http.Request, object runtime.Object, err error) *http.Response {
	statusCode := http.StatusOK
	var payload any = object
	if err != nil {
		statusCode = http.StatusInternalServerError
		if status, ok := err.(apierrors.APIStatus); ok {
			statusCode = int(status.Status().Code)
		}
		payload = &metav1.Status{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
			Status:   metav1.StatusFailure,
			Message:  err.Error(),
			Code:     int32(statusCode),
		}
	}
	body, marshalErr := json.Marshal(payload)
	if marshalErr != nil {
		statusCode = http.StatusInternalServerError
		body = []byte(marshalErr.Error())
	}
	return &http.Response{
		StatusCode: statusCode,
		Status:     fmt.Sprintf("%d", statusCode),
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(bytes.NewReader(body)),
		Request:    req,
	}
}

func statusService(synthetic bool) *v1.Service {
	policy := v1.IPFamilyPolicySingleStack
	annotations := map[string]string{}
	if synthetic {
		annotations["development.kube-vip.io/synthetic-api-server-error-on-update"] = "true"
	}
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "status-service",
			Namespace:   "default",
			UID:         "status-service-uid",
			Annotations: annotations,
		},
		Spec: v1.ServiceSpec{
			Type:           v1.ServiceTypeLoadBalancer,
			LoadBalancerIP: "192.0.2.10",
			IPFamilyPolicy: &policy,
		},
	}
}

func statusInstance(service *v1.Service) *instance.Instance {
	return &instance.Instance{
		ServiceSnapshot: service.DeepCopy(),
		VIPConfigs: []*kubevip.Config{{
			VIP: "192.0.2.10",
		}},
	}
}

func TestUpdateStatusRetriesReactorFailureWithoutDroppingService(t *testing.T) {
	service := statusService(false)
	client, fakeClient := newFakeBackedClient(t, service)
	var attempts atomic.Int32
	fakeClient.PrependReactor("update", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "status" {
			return false, nil, nil
		}
		if attempts.Add(1) == 1 {
			return true, nil, apierrors.NewInternalError(errors.New("synthetic status reactor failure"))
		}
		return false, nil, nil
	})

	p := &Processor{config: &kubevip.Config{}, clientSet: client}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.updateStatus(ctx, statusInstance(service)); err != nil {
		t.Fatalf("updateStatus returned error after a transient failure: %v", err)
	}
	if got := attempts.Load(); got < 2 {
		t.Fatalf("status update attempts = %d, want at least 2", got)
	}

	updated, err := fakeClient.CoreV1().Services(service.Namespace).Get(context.Background(), service.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("service disappeared after status retry: %v", err)
	}
	if updated.UID != service.UID || len(updated.Status.LoadBalancer.Ingress) != 1 {
		t.Fatalf("status retry did not preserve the service or write ingress: %#v", updated)
	}
}

func TestUpdateStatusSyntheticFailureRetriesWithoutPanic(t *testing.T) {
	service := statusService(true)
	client, fakeClient := newFakeBackedClient(t, service)
	var gets atomic.Int32
	var firstGetHadSynthetic atomic.Bool
	fakeClient.PrependReactor("get", "services", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if _, ok := action.(k8stesting.GetAction); !ok {
			return false, nil, nil
		}
		attempt := gets.Add(1)
		// Reactors run while the fake client's tracker is locked, so do not read it here.
		serviceCopy := service.DeepCopy()
		if attempt == 1 {
			firstGetHadSynthetic.Store(serviceCopy.Annotations["development.kube-vip.io/synthetic-api-server-error-on-update"] == "true")
			return false, nil, nil
		}
		delete(serviceCopy.Annotations, "development.kube-vip.io/synthetic-api-server-error-on-update")
		return true, serviceCopy, nil
	})

	p := &Processor{config: &kubevip.Config{}, clientSet: client}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.updateStatus(ctx, statusInstance(service)); err != nil {
		t.Fatalf("updateStatus returned error after synthetic retry: %v", err)
	}
	if !firstGetHadSynthetic.Load() {
		t.Fatal("synthetic update failure was not exercised")
	}
	if got := gets.Load(); got < 2 {
		t.Fatalf("service GET attempts = %d, want at least 2 after synthetic failure", got)
	}
	if _, err := fakeClient.CoreV1().Services(service.Namespace).Get(context.Background(), service.Name, metav1.GetOptions{}); err != nil {
		t.Fatalf("service disappeared after synthetic failure: %v", err)
	}
}
