# Contributor Guide

## Platform and toolchain

- Use the Go version declared by `go.mod`. The Makefile's containerized test target pins the same toolchain.
- Run Go unit and integration tests on Linux. Network, netlink, nftables, iptables, namespaces, and kind-based E2E behavior cannot be validated faithfully on macOS.
- Run E2E suites on a Linux host with Docker, kind, kubectl, and sufficient inotify limits. The CI workflows are the reference environment.

## Required checks

- Format Go: `gofmt -w .`
- Full validation: `make check`
- Unit tests with race detection: `make unit-tests`
- Etcd integration tests: `make integration-tests`
- Mode E2E: `make dockerx86Local` followed by one or more of `make e2e-tests-arp`, `make e2e-tests-rt`, and `make e2e-tests-bgp`
- Services E2E: `DOCKERTAG=action make dockerx86ActionIPTables service-tests`

Before running a workflow locally, inspect its current Make target and Ginkgo arguments. Before editing an E2E spec, confirm the target includes that file and that build tags and label filters do not silently exclude it.

## Test isolation

- kube-vip uses host networking and its metrics listener defaults to `:2112`. Concurrent pods on one node will collide on that port.
- Standard E2E templates disable metrics with `--prometheusHTTPServer ""`. Enable metrics only for the pod or phase that scrapes them, and wait for earlier metrics-enabled pods to terminate before reusing the port.
- Always clean up kind clusters, containers, temporary manifests, network rules, and fault injection in teardown paths, including after failures.

## Change structure

- Do not mix test-only changes with production fixes. If a test exposes a bug, put the fix and its focused regression test in a separate production PR.
- Keep stacked branches based on their declared parent, not independently rebased onto `main`. When a parent changes, restack descendants in ancestry order and verify each PR's three-dot diff contains only that PR's intended delta.
- Before pushing a stacked branch, verify its merge base, target branch, changed-file list, and commits unique to the branch.
