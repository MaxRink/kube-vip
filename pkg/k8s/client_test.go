package k8s

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRestConfigUsesExplicitKubeconfig(t *testing.T) {
	path := writeTestKubeconfig(t)
	timeout := 2 * time.Second

	config, err := restConfig(path, false, timeout)
	if err != nil {
		t.Fatalf("restConfig() error = %v", err)
	}
	if config.Host != "https://127.0.0.1:6443" {
		t.Errorf("restConfig() host = %q, want kubeconfig server", config.Host)
	}
	if config.QPS != 100 {
		t.Errorf("restConfig() QPS = %v, want 100", config.QPS)
	}
	if config.Burst != 250 {
		t.Errorf("restConfig() burst = %d, want 250", config.Burst)
	}
	if config.Timeout != timeout {
		t.Errorf("restConfig() timeout = %v, want %v", config.Timeout, timeout)
	}
}

func TestNewRestConfigHostnameOverride(t *testing.T) {
	path := writeTestKubeconfig(t)
	override := "https://api.example.test:6443"

	config, err := NewRestConfig(path, false, override)
	if err != nil {
		t.Fatalf("NewRestConfig() error = %v", err)
	}
	if config.Host != override {
		t.Errorf("NewRestConfig() host = %q, want %q", config.Host, override)
	}
}

func TestRestConfigPathFallbacks(t *testing.T) {
	validPath := writeTestKubeconfig(t)
	missingPath := filepath.Join(t.TempDir(), "missing-kubeconfig")

	tests := []struct {
		name       string
		path       string
		inCluster  bool
		wantErr    bool
		errContain string
	}{
		{name: "explicit temporary file", path: validPath},
		{name: "missing explicit file", path: missingPath, wantErr: true, errContain: "failed to build config from file"},
		{name: "no path outside cluster", wantErr: true, errContain: "path to KubeConfig not specified"},
		{name: "in-cluster fallback without service account", inCluster: true, wantErr: true, errContain: "failed to get incluster config"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := restConfig(tt.path, tt.inCluster, time.Second)
			if (err != nil) != tt.wantErr {
				t.Fatalf("restConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil && tt.errContain != "" && !strings.Contains(err.Error(), tt.errContain) {
				t.Errorf("restConfig() error = %q, want substring %q", err, tt.errContain)
			}
		})
	}
}

func writeTestKubeconfig(t *testing.T) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "config")
	data := []byte(`apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:6443
users:
- name: test
  user:
    token: test-token
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("writing temporary kubeconfig: %v", err)
	}
	return path
}
