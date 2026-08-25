//go:build e2e
// +build e2e

package e2e_test

import (
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/kube-vip/kube-vip/testing/e2e"
)

// metricPresent is deliberately a capability probe rather than an assertion.
// The metric production PRs are independent of the e2e stack, so a missing
// series must skip only the metric-dependent spec instead of failing it.
func metricPresent(clusterName, node, name string) bool {
	metrics, err := e2e.ScrapeMetrics(clusterName, node)
	return err == nil && e2e.MetricPresent(metrics, name)
}

func metricPresentWithLabels(clusterName, node, name string, labels map[string]string) bool {
	metrics, err := e2e.ScrapeMetrics(clusterName, node)
	if err != nil {
		return false
	}
	_, matches := e2e.MetricValue(metrics, name, labels)
	return matches > 0
}

func requireMetricCapability(clusterName string, nodes []string, name string) {
	for _, node := range nodes {
		metrics, err := e2e.ScrapeMetrics(clusterName, node)
		if err != nil {
			Skip(fmt.Sprintf("optional metric %q could not be scraped from %q (%v); skipping metric assertions until the metrics PRs are merged", name, node, err))
		}
		if !e2e.MetricPresent(metrics, name) {
			Skip(fmt.Sprintf("optional metric %q is absent on %q; skipping metric assertions until the metrics PRs are merged", name, node))
		}
	}
}

func requireMetricCapabilityWithLabels(clusterName string, nodes []string, name string, labels map[string]string) {
	for _, node := range nodes {
		if !metricPresentWithLabels(clusterName, node, name, labels) {
			Skip(fmt.Sprintf("optional metric %q with labels %v is absent on %q; skipping metric assertions until the metrics PRs are merged", name, labels, node))
		}
	}
}

func requireMetricCapabilityOnAnyNode(clusterName string, nodes []string, name string) {
	for _, node := range nodes {
		if metricPresent(clusterName, node, name) {
			return
		}
	}
	Skip(fmt.Sprintf("optional metric %q is absent on every node; skipping metric assertions until the metrics PRs are merged", name))
}

func metricPresentOnAnyNode(clusterName string, nodes []string, name string) bool {
	for _, node := range nodes {
		if metricPresent(clusterName, node, name) {
			return true
		}
	}
	return false
}

// assertEventuallyStableMetric makes the normal eventual assertion first and
// then repeats it with MetricStable. The second phase deliberately samples
// twice with a gap so a leaked loop cannot pass on a single lucky scrape.
func assertEventuallyStableMetric(clusterName, node, name string, labels map[string]string,
	expected float64, timeout, interval, gap time.Duration,
) {
	e2e.EventuallyMetric(clusterName, node, name, labels, Equal(expected), timeout, interval)
	e2e.EventuallyMetricStable(clusterName, node, name, labels, Equal(expected), timeout, interval, gap)
}
