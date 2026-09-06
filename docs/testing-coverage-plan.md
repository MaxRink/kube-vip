# Test Coverage Expansion Plan

This roadmap tracks a plan PR plus 18 implementation PRs for fault injection, configuration and lifecycle unit coverage, combination testing, scale testing, and observability. It describes both the repository as it exists now and work proposed on the fork; proposed items are not claims about features already present on `main`.

## Current baseline

As of 2026-09-06, the plan is based on upstream `main` at `2bc2df5`.

### Present on upstream main

- Ginkgo v2 and kind cover ARP, routing-table, and BGP E2E modes. Pull-request CI runs all three on Linux with four Ginkgo processes per mode, plus the services E2E suite.
- PRs #1698, #1699, #1701, and #1702 are merged. Their parallel CI plumbing, endpoint-provider split, active-endpoint ownership, failover assertions, report paths, and formatting checks are prerequisites no longer carried by this plan.
- `make unit-tests` runs `go test -race ./...`; `make integration-tests` runs the tagged etcd package tests. The kind E2E suites require Linux behavior and are not expected to run faithfully on a macOS host.
- There are 30 top-level directories under `pkg/`; 17 contain tests, with 49 `*_test.go` files under `pkg/`. Counting directories is only a coarse debt signal: several tested packages still have important untested lifecycle paths.
- Prometheus is served at `/metrics`; the command-line listener defaults to `:2112`. Standard E2E pod templates explicitly pass `--prometheusHTTPServer ""`, so their stale `prometheus_server` environment values do not enable metrics. Metrics must be enabled selectively for suites that scrape them because host-networked kube-vip pods on one node otherwise contend for port 2112.
- Existing metrics cover active services, reconciliation errors and duration, service watch events, leader state and transitions, per-service election loops/attempts/errors, BGP session state, and build information.
- Fault coverage is still mostly process or control-plane disruption in existing suites. There is no complete network-partition, pairwise configuration, or controller-scale suite on upstream `main`.

### In progress or proposed

- Upstream PR #1705 remains open and is the authoritative config-loader correction. Its production changes and corrected tests are not present on `main` and are not part of the roadmap baseline.
- The fork has implemented branches for much of this roadmap, but open fork PRs are not described as current upstream behavior. They must remain reviewable, independently verified deltas.
- Production observability additions are isolated from test-only changes. Tests may capability-check metrics until the corresponding metrics PR is available, but must not silently weaken assertions where the metric is required by their target branch.

## Delivery rules

- Keep each implementation PR focused, reviewable, and independently revertible.
- Never combine a production bug fix with an otherwise test-only PR. Land the failing or documenting test separately when practical, then put production behavior and its focused regression test in a `fix:` PR.
- Run unit, lint, and relevant Linux E2E checks before a branch is considered ready. A fault test must be shown to pass on corrected code and fail on the relevant known-bad revision when that revision remains buildable.
- For stacked work, preserve parent ancestry. Restack descendants in order after a parent changes, and inspect each three-dot diff against its declared target before pushing.
- Preflight workflow changes by checking event triggers, permissions, matrix dimensions, timeouts, artifact paths, and the exact Make target invoked. Preflight E2E changes by checking build tags, Ginkgo labels, focus/skip filters, and whether the intended spec is selected.
- Every fault helper must register cleanup before injecting the fault. Remove kind clusters, containers, temporary files, iptables/nftables rules, and network disconnects even when a spec fails.

## Delivery map

The roadmap consists of this plan and 18 implementation PRs. Numbers below are roadmap identifiers, not fork pull-request numbers.

### Foundation and unit coverage

1. **PR-1, coverage and nightly skeleton:** publish unit coverage, wire the existing etcd integration suite into a scheduled/dispatch workflow, and retain artifacts. Do not duplicate CI parallelism already delivered by #1698/#1702.
2. **PR-2, config loader tests:** test real environment and file loader behavior. Classify fields as mergeable, runtime-derived, generator-only, or bootstrap-only, with reasons; avoid a false invariant that every zero value must overwrite a configured default. Keep production corrections in upstream #1705 or a successor fix.
3. **PR-3, E2E metrics helpers:** add parser and polling helpers for `/metrics`, then assert existing leader, active-service, and BGP-session metrics in selected green suites. Enable `:2112` only for the tested workload.
4. **PR-4, services lifecycle tests:** cover processor and service-context add/update/delete, cancellation, status failures, and idempotent cleanup with fake Kubernetes clients where appropriate.
5. **PR-5, election and lease tests:** cover acquisition, release, recreation after cancellation, shared lease names, restart behavior, and callback ordering.
6. **PR-6, cluster and instance tests:** cover VIP worker startup/rollback, shutdown concurrency, leadership-loss cleanup, address resolution, and preserve semantics supported by current production code.
7. **PR-7, BGP and endpoint-provider tests:** cover peer translation, MP-BGP family behavior and fallback, and parity between EndpointSlice and legacy Endpoints providers.
8. **PR-15, unit parallelization:** add shuffling and safe `t.Parallel` use only after measuring current CI. Do not parallelize tests that share process environment, global metrics, ports, or network state.

These branches are independent and target `main`: PR-1, PR-2, PR-4, PR-5, PR-6, PR-7, and PR-15.

### Fault, matrix, nightly, and scale

9. **PR-8, fault primitives:** provide reversible node partition, API blackhole, process signal, node restart, and lease mutation helpers. Centralize existing static-pod manifest stashing.
10. **PR-9, control-plane faults:** exercise API loss, process death, lease deletion or theft, and node restart in ARP/routing-table modes, with functional and steady-state metric assertions.
11. **PR-10, service faults:** exercise global, per-service, and on-demand election through API loss, leader death, deletion, endpoint churn, egress, and DNS refresh scenarios.
12. **PR-11, pairwise matrix:** generate deterministic pairwise coverage across supported mode, function, IP family, election, deployment shape, endpoint provider, and traffic-policy axes, with explicit exclusions.
13. **PR-12, nightly wiring:** run fault, matrix, etcd, and services suites on schedule and dispatch, with bounded jobs and always-uploaded diagnostics.
14. **PR-13, controller scale:** test bounded service creation, churn, election churn, and endpoint fan-out on Linux kind runners. Measure convergence and leaked state rather than dataplane throughput.

Current fork ancestry is intentionally split after the control-plane fault suite:

```text
PR-3 -> PR-8 -> PR-9 -> PR-10
                    |-> PR-11 -> PR-14d
                    |-> PR-13
PR-1 -----------------> PR-12
```

PR-12 targets PR-1 because it extends the nightly workflow. PR-10, PR-11, and PR-13 each target PR-9 and are siblings, not a single chain.

### Observability

15. **PR-14a, loop liveness:** add watcher and election loop gauges at lifecycle boundaries, with exact increment/decrement tests.
16. **PR-14b, dataplane operations:** add VIP, ARP/NDP, route, DNS, and DHCP state/operation metrics with bounded labels.
17. **PR-14c, BGP and egress:** add route, peer, egress/nftables, and watcher-restart metrics with transition tests.
18. **PR-14d, metric assertions:** use the new metrics after faults and matrix cases, cross-check reported VIP state with node state, and verify counters settle after recovery.

Current fork ancestry is:

```text
PR-14a -> PR-14b -> PR-14c
PR-11  -> PR-14d
```

PR-14d may capability-check independently developed metrics while the production stack is unmerged, but final required assertions must be enabled when their metric ancestors are present.

## Acceptance criteria

- Unit changes pass `make check` and `make unit-tests` on Linux.
- Etcd changes pass `make integration-tests`.
- E2E changes pass the affected `make e2e-tests-arp`, `make e2e-tests-rt`, `make e2e-tests-bgp`, or `make service-tests` target on Linux and produce usable failure artifacts.
- Matrix generation has deterministic unit tests proving required pair coverage and exclusions.
- Fault helpers are idempotent and cleanup-safe; fault specs establish a stable post-recovery window instead of asserting a transient sample.
- Scale jobs remain within documented node, service, API-QPS, runtime, and artifact limits suitable for hosted Linux runners.
- Each branch diff contains only its declared roadmap item and maintains the ancestry shown above.
