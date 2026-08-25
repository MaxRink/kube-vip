package bgp

import (
	"net"
	"reflect"
	"strings"
	"testing"

	"github.com/kube-vip/kube-vip/pkg/kubevip"
	api "github.com/osrg/gobgp/v4/api"
)

func TestPeerConfigTranslation(t *testing.T) {
	t.Parallel()
	// Parsing an unnumbered peer is pure. Resolving its link-local neighbor
	// requires live interface and neighbor state, so getIPv6LinkLocalNeighborAddress
	// is intentionally not called by this unit test.

	tests := []struct {
		name   string
		config string
		want   kubevip.BGPPeer
	}{
		{
			name:   "ipv4 peer with explicit port and multihop",
			config: "192.0.2.10:65001:secret:true:180",
			want: kubevip.BGPPeer{
				Address:             "192.0.2.10",
				Port:                180,
				AS:                  65001,
				Password:            "secret",
				MultiHop:            true,
				BFDReceiveInterval:  300,
				BFDTransmitInterval: 300,
				BFDDetectMultiplier: 3,
			},
		},
		{
			name:   "bracketed ipv6 peer",
			config: "[2001:db8::10]:65002:secret:false:181",
			want: kubevip.BGPPeer{
				Address:             "2001:db8::10",
				Port:                181,
				AS:                  65002,
				Password:            "secret",
				BFDReceiveInterval:  300,
				BFDTransmitInterval: 300,
				BFDDetectMultiplier: 3,
			},
		},
		{
			name:   "unnumbered interface peer",
			config: "unnumbered:eth1:65003:secret:true:182",
			want: kubevip.BGPPeer{
				Interface:           "eth1",
				Port:                182,
				AS:                  65003,
				Password:            "secret",
				MultiHop:            true,
				BFDReceiveInterval:  300,
				BFDTransmitInterval: 300,
				BFDDetectMultiplier: 3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := kubevip.ParseBGPPeerConfig(tt.config)
			if err != nil {
				t.Fatalf("ParseBGPPeerConfig() error = %v", err)
			}
			if len(got) != 1 {
				t.Fatalf("ParseBGPPeerConfig() returned %d peers, want 1", len(got))
			}
			if !reflect.DeepEqual(got[0], tt.want) {
				t.Errorf("ParseBGPPeerConfig() = %+v, want %+v", got[0], tt.want)
			}
		})
	}
}

func TestBGPPathTranslation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		ip     string
		family string
		prefix string
	}{
		{name: "ipv4", ip: "192.0.2.10", family: "ipv4-unicast", prefix: "192.0.2.10/32"},
		{name: "ipv6", ip: "2001:db8::10", family: "ipv6-unicast", prefix: "2001:db8::10/128"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := (&Server{}).getPath(net.ParseIP(tt.ip))
			if path == nil {
				t.Fatal("getPath() returned nil")
			}
			if got := path.Family.String(); got != tt.family {
				t.Errorf("getPath() family = %q, want %q", got, tt.family)
			}
			if got := path.Nlri.String(); got != tt.prefix {
				t.Errorf("getPath() prefix = %q, want %q", got, tt.prefix)
			}
			if got := len(path.Attrs); got != 2 {
				t.Errorf("getPath() attributes = %d, want 2", got)
			}
		})
	}
}

func TestMPBGPNexthopTranslation(t *testing.T) {
	t.Parallel()

	serverConfig := kubevip.BGPConfig{
		SourceIP:     "192.0.2.20",
		SourceIF:     "eth1",
		MpbgpNexthop: "fixed",
		MpbgpIPv4:    "198.51.100.20",
		MpbgpIPv6:    "2001:db8::20",
	}
	peer := kubevip.BGPPeer{
		MpbgpIPv4: "203.0.113.20",
	}
	ap := &api.Peer{Transport: &api.Transport{}}

	ipv4, ipv6, err := peer.FindMpbgpAddresses(ap, &serverConfig)
	if err != nil {
		t.Fatalf("FindMpbgpAddresses() error = %v", err)
	}
	if ipv4 != peer.MpbgpIPv4 {
		t.Errorf("IPv4 next hop = %q, want %q", ipv4, peer.MpbgpIPv4)
	}
	if ipv6 != serverConfig.MpbgpIPv6 {
		t.Errorf("IPv6 next hop = %q, want %q", ipv6, serverConfig.MpbgpIPv6)
	}
	if got := ap.Transport.LocalAddress; got != serverConfig.SourceIP {
		t.Errorf("fixed mode local address = %q, want %q", got, serverConfig.SourceIP)
	}

	peer.SetMpbgpOptions(&serverConfig)
	if peer.MpbgpNexthop != serverConfig.MpbgpNexthop {
		t.Errorf("peer MP-BGP mode = %q, want %q", peer.MpbgpNexthop, serverConfig.MpbgpNexthop)
	}
	if peer.MpbgpIPv6 != serverConfig.MpbgpIPv6 {
		t.Errorf("peer IPv6 option = %q, want %q", peer.MpbgpIPv6, serverConfig.MpbgpIPv6)
	}
}

func TestMPBGPNexthopModesWithoutHostInterfaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		mode      string
		config    kubevip.BGPConfig
		wantField string
	}{
		{
			name: "auto source ip",
			mode: "auto_sourceip",
			config: kubevip.BGPConfig{
				SourceIP: "not-an-ip",
			},
			wantField: "local address",
		},
		{
			name: "auto source interface",
			mode: "auto_sourceif",
			config: kubevip.BGPConfig{
				SourceIF: "kube-vip-test-interface-does-not-exist",
			},
			wantField: "bind interface",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			peer := kubevip.BGPPeer{MpbgpNexthop: tt.mode}
			ap := &api.Peer{Transport: &api.Transport{}}
			_, _, err := peer.FindMpbgpAddresses(ap, &tt.config)
			if err == nil {
				t.Fatal("FindMpbgpAddresses() error = nil, want interface lookup error")
			}

			switch tt.wantField {
			case "local address":
				if got := ap.Transport.LocalAddress; got != tt.config.SourceIP {
					t.Errorf("auto_sourceip local address = %q, want %q", got, tt.config.SourceIP)
				}
			case "bind interface":
				if got := ap.Transport.BindInterface; got != tt.config.SourceIF {
					t.Errorf("auto_sourceif bind interface = %q, want %q", got, tt.config.SourceIF)
				}
			}
		})
	}
}

func TestMPBGPNexthopRejectsInvalidFixedConfiguration(t *testing.T) {
	t.Parallel()

	tests := []kubevip.BGPPeer{
		{MpbgpNexthop: "fixed"},
		{MpbgpNexthop: "fixed", MpbgpIPv4: "not-an-ip"},
		{MpbgpNexthop: "unsupported"},
	}
	for _, peer := range tests {
		ap := &api.Peer{Transport: &api.Transport{}}
		_, _, err := peer.FindMpbgpAddresses(ap, &kubevip.BGPConfig{})
		if err == nil {
			t.Errorf("FindMpbgpAddresses(%q) error = nil, want error", peer.MpbgpNexthop)
			continue
		}
		if strings.TrimSpace(err.Error()) == "" {
			t.Errorf("FindMpbgpAddresses(%q) returned an empty error", peer.MpbgpNexthop)
		}
	}
}
