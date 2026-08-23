package instance

import (
	"context"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/kube-vip/kube-vip/pkg/kubevip"
	"github.com/kube-vip/kube-vip/pkg/networkinterface"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestFetchServiceAddresses_PrecedenceAndOrdering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		annotations map[string]string
		status      []v1.LoadBalancerIngress
		specIP      string
		wantIPs     []string
		wantHosts   []string
	}{
		{
			name: "annotation wins and preserves dual-stack IP order",
			annotations: map[string]string{
				kubevip.LoadbalancerIPAnnotation: " 192.0.2.10, 2001:0db8::10, vip.example",
			},
			status: []v1.LoadBalancerIngress{
				{IP: "192.0.2.20"},
			},
			specIP:    "192.0.2.30",
			wantIPs:   []string{"192.0.2.10", "2001:db8::10"},
			wantHosts: []string{"vip.example"},
		},
		{
			name: "status hostname takes precedence over legacy spec address",
			status: []v1.LoadBalancerIngress{
				{Hostname: "status.example"},
			},
			specIP:    "192.0.2.30",
			wantIPs:   []string{},
			wantHosts: []string{"status.example"},
		},
		{
			name: "status preserves dual-stack ingress ordering",
			status: []v1.LoadBalancerIngress{
				{IP: "192.0.2.20"},
				{IP: "2001:db8::20"},
				{Hostname: "status.example"},
			},
			wantIPs:   []string{"192.0.2.20", "2001:db8::20"},
			wantHosts: []string{"status.example"},
		},
		{
			name:      "legacy spec address is the final fallback",
			specIP:    "192.0.2.30",
			wantIPs:   []string{"192.0.2.30"},
			wantHosts: []string{},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Annotations: tc.annotations},
				Spec: v1.ServiceSpec{
					LoadBalancerIP: tc.specIP,
				},
				Status: v1.ServiceStatus{
					LoadBalancer: v1.LoadBalancerStatus{Ingress: tc.status},
				},
			}

			gotIPs, gotHosts := FetchServiceAddresses(svc)
			if !reflect.DeepEqual(gotIPs, tc.wantIPs) {
				t.Fatalf("addresses = %#v, want %#v", gotIPs, tc.wantIPs)
			}
			if !reflect.DeepEqual(gotHosts, tc.wantHosts) {
				t.Fatalf("hostnames = %#v, want %#v", gotHosts, tc.wantHosts)
			}
		})
	}
}

func TestNewInstance_SetsDualStackFlagsFromServiceSpec(t *testing.T) {
	requireLinuxNetlink(t)

	tests := []struct {
		name            string
		families        []v1.IPFamily
		policy          v1.IPFamilyPolicy
		wantDualStack   bool
		wantRequireDual bool
	}{
		{
			name:            "require dual stack",
			families:        []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
			policy:          v1.IPFamilyPolicyRequireDualStack,
			wantDualStack:   true,
			wantRequireDual: true,
		},
		{
			name:            "prefer dual stack",
			families:        []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
			policy:          v1.IPFamilyPolicyPreferDualStack,
			wantDualStack:   true,
			wantRequireDual: false,
		},
		{
			name:            "single stack",
			families:        []v1.IPFamily{v1.IPv4Protocol},
			policy:          v1.IPFamilyPolicySingleStack,
			wantDualStack:   false,
			wantRequireDual: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			policy := tc.policy
			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						kubevip.LoadbalancerIPAnnotation: "192.0.2.10",
					},
				},
				Spec: v1.ServiceSpec{
					IPFamilies:     tc.families,
					IPFamilyPolicy: &policy,
				},
			}
			cfg := &kubevip.Config{Interface: "lo"}

			got, err := NewInstance(context.Background(), svc, cfg, networkinterface.NewManager(), nil, nil, nil, &sync.WaitGroup{})
			if err != nil {
				t.Fatalf("NewInstance: %v", err)
			}
			if len(got.VIPConfigs) != 1 {
				t.Fatalf("VIP config count = %d, want 1", len(got.VIPConfigs))
			}
			if got.VIPConfigs[0].IsDualStack != tc.wantDualStack {
				t.Errorf("IsDualStack = %t, want %t", got.VIPConfigs[0].IsDualStack, tc.wantDualStack)
			}
			if got.VIPConfigs[0].RequireDualStack != tc.wantRequireDual {
				t.Errorf("RequireDualStack = %t, want %t", got.VIPConfigs[0].RequireDualStack, tc.wantRequireDual)
			}
		})
	}
}

func TestNewInstance_RejectsDHCPWithMoreThanTwoAddresses(t *testing.T) {
	requireLinuxNetlink(t)

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				kubevip.LoadbalancerIPAnnotation: "0.0.0.0, ::, 192.0.2.10",
			},
		},
	}
	_, err := NewInstance(context.Background(), svc, &kubevip.Config{Interface: "lo"},
		networkinterface.NewManager(), nil, nil, nil, &sync.WaitGroup{})
	if err == nil {
		t.Fatal("expected DHCP address-count constraint error")
	}
	if !strings.Contains(err.Error(), "DHCP cannot be used if more than 2 addresses") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAutoFindInterfaceAndSubnet_RequiresNetlinkFixture(t *testing.T) {
	t.Skip("autoFindInterface and autoFindSubnet call package-global netlink APIs; deterministic fixtures require an injectable netlink seam and belong in Linux integration/e2e coverage")
}

func requireLinuxNetlink(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("NewInstance reaches package-global netlink APIs; these tests require Linux netlink state")
	}
}
