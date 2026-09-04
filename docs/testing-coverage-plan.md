# kube-vip: Test Coverage Expansion (Fault Injection, Matrix, Scale)

## Context

Repeated regressions have shipped from PRs over recent months. A 12-month fix-commit analysis shows the hotspots, ranked:

1. **Services controller + endpoints + per-service leader election** (`pkg/services`, `pkg/endpoints`, `pkg/servicecontext`, `pkg/lease`): context/lease/goroutine lifecycle bugs, mostly fallout from the #1463 refactor (e.g. #1664 lease recreation after cancelled context, #1477 election restart, #1561/#1553 deletion with global election, #1511 node watcher restart).
2. **Control-plane election + VIP lifecycle** (`pkg/cluster`): #1594 AddIP deadlock, IP deletion on leadership loss, #1603 etcd-mode restart.
3. **BGP** (gobgp v3→v4 fallout, metrics fixed 3x, unnumbered peers reverted+relanded).
4. **Egress/nftables** (rule scoping, SNAT, table isolation).
5. **DNS/FQDN VIP refresh** (small but chronic: #1667, #1390, ...).

Signature failure modes: loop tied to a cancelled context, lease not recreated, duplicate/missed delete on leadership transition, watcher not restarted after error. Many of these are **functionally invisible** (VIP still serves, lease has a holder), so functional e2e assertions alone cannot guard them; internal observability (loop-liveness gauges) is required.

Current state: Ginkgo v2 + kind harness in `testing/e2e` covering ARP/RT/BGP happy paths; fault injection limited to `docker kill` of the leader and apiserver manifest-stash; no network partition, no scale tests, no coverage measurement; ~20 of 32 `pkg/` packages have zero unit tests; no `client-go` fake clientset usage anywhere; etcd e2e suite not in CI; the env/file config merge (`pkg/kubevip/config_environment.go:873-1133`) is missing branches for ~10 fields.

### Baseline: in-flight PRs this plan builds on (as of 2026-08-22)

- **#1698 (cellebyte, MERGED)** "Refactor/testing to speedup CI in Pull Requests": already delivered `fail-fast: false`, `max-parallel: 3`, configurable ginkgo procs (`GINKGO_PROCS`/`GINKGO_ARGS` in Makefile), ginkgo JSON reports, PR concurrency cancellation, buildx cache flags, parallelized mode scenarios in `testing/e2e`, a refactored phase-parallel `testing/services` harness, and production changes (egress CIDR discovery from k8s APIs, VIP teardown when Service leaves LoadBalancer type). **The local checkout is behind; rebase all work onto origin/main (bee5cfe).**
- **#1702 (MaxRink, OPEN)** follow-up fixing #1698 review findings: egress CIDR kcm-flag fallback + SNAT exclusion of all node pod CIDRs, harness metrics isolation (`--prometheusHTTPServer` arg instead of ineffective env var), failover assertions that previously let a dead VIP pass CI, 3-interval lease-error stability check, ginkgo report `--output-dir`, repo-wide gofmt gate, concurrency scoped to PRs + tag builds restored.
- **#1701 (cellebyte, OPEN)** "fix(egress): prevent stale active-endpoint overwrite": makes the endpoint watcher the sole active-endpoint annotation writer; adds/extends `pkg/endpoints/endpoints_test.go` and `pkg/services/services_test.go`.
- **#1699 (cellebyte, OPEN)** "refactor/endpoints for different modes": splits `pkg/endpoints` into per-mode files (`endpoints_bgp.go`, `endpoints_generic.go`, `endpoints_routing_table.go`, `endpoints_wireguard.go`), touches providers and `pkg/manager/worker`.

Consequences: parts of the original Phase 1 (fail-fast, ginkgo parallelism plumbing) are done; unit-test PRs touching `pkg/services`/`pkg/endpoints` must sequence after #1702/#1701/#1699 to avoid conflicting with active refactors; the `testing/services` harness now has phase-parallel structure and (via #1702) strict failover assertions, which this plan extends rather than introduces.

## Decisions (agreed with user)

- **CI placement**: fast PR-blocking subset stays (~30 min); full fault + matrix + scale runs nightly, on `workflow_dispatch`, and via an `e2e-full` PR label.
- **Scale environment**: kind on standard GitHub runners; controller-behavior scale (service count, churn, election churn, endpoint fan-out), not dataplane throughput.
- **Combination depth**: pairwise over key axes via a small in-repo generator; curated deep scenarios for hotspots.
- **Delivery**: granular, individually reviewable PRs (each <~30 min review, CI-green, revertable). 15 of 18 are test/CI-only; the three production-code PRs (metrics, Phase 6) are isolated, strictly additive, and split by subsystem.
- **Metrics coverage**: every subsystem must be observable via Prometheus metrics; fault/matrix/scale suites assert on metrics, not just functional behavior (many historical regressions were functionally invisible).
- **Parallelisation**: unit and e2e suites are parallelised and sharded so the added coverage does not grow PR wall time; target is PR e2e dropping from ~30 min to <=~18 min despite more tests.

## Delivery process (applies to every PR)

- **PRs target the fork, not upstream (for now).** All PRs open against `MaxRink/kube-vip` (the user's fork) to avoid overloading upstream maintainers; upstream submission happens later as a curated batch once the stack has proven itself on the fork's CI. Pre-flight: verify the fork exists and has Actions enabled so draft CI actually runs.
- **The plan itself is PR #0.5.** Commit this plan (as e.g. `docs/testing-coverage-plan.md`) to the fork in its own separate PR before implementation starts, so progress is trackable and reviewable in-repo.
- **Codex subagents wherever possible.** Implementation, diagnosis, and second-pass review are delegated to codex subagents (codex:codex-rescue / codex:rescue) whenever the task is codeable and verifiable; Claude subagents (go-test-writer, pr-review-toolkit:*) fill roles codex does not cover. The main session orchestrates, verifies, and handles git only.
- **Intermediate plans, codex-verifiable.** Before implementing each PR, write a detailed per-PR plan file (`tasks/pr-plans/PR-XX.md`): exact files, function-level scope, and an acceptance checklist of concrete commands with expected outcomes (`go test ./pkg/services -run TestX` passes, `ginkgo --label-filter=faults` green, generator emits all pairs). Codex implements against that plan and verifies the checklist directly; the checklist result goes into the PR description.
- **Everything locally tested before pushing** (wherever runnable). Unit tests always. E2E: run the relevant `make e2e-tests-*` locally via Docker/kind (macOS: test container with `--network host`). Where the macOS host can't run a suite faithfully or lacks resources (parallel multi-cluster suites, scale suite), provision an OpenStack Linux VM (existing DT OpenStack tooling/Terraform) as a dedicated test runner, run the suite there, and record the result in the PR. Only push after the local/VM run is green.
- **Parallel implementation.** Independent PRs (PR-1, PR-2, PR-4..7, PR-15, PR-14a..c) are implemented concurrently by parallel codex subagents in isolated git worktrees; the stacked chain is built sequentially but its per-PR plans are written ahead so the next link starts the moment its parent is stable.
- **Bugs found by tests go to separate PRs.** A test PR never mixes in production fixes. When a new test unearths a bug: document it in the test PR (skip-with-comment naming the defect and tracking issue, or allowlist entry like PR-2's `knownUnmergedFields`), then fix it in its own stacked `fix:` PR that also un-skips the test. Never delete a failing fault test because of an unrelated pre-existing bug.
- **Subagent review gate before ready.** Before a draft is marked ready: codex review pass plus Claude review subagents appropriate to the PR (pr-review-toolkit:code-reviewer always; pr-test-analyzer for test PRs; silent-failure-hunter for fault-handling code; operator-reviewer for production metrics PRs), fix findings, and only then mark ready.
- **Always create PRs as drafts** (`gh pr create --draft`). Mark ready only after: local/VM test run green, CI green on the draft, review findings addressed, and for fault specs the bidirectional validation (pass on fixed code, fail on known-bad commit) documented in the PR description.
- **Use GitHub native stacked PRs for the dependent chain.** The chain PR-3 → PR-8 → PR-9 → {PR-10, PR-11} → PR-12 → PR-13 → PR-14d is managed as a gh stack (via the stacked-prs skill: create/restack/land) on the fork, so each PR's diff shows only its own changes and retargeting after a merge is handled by the tooling. Independent PRs target the fork's main directly.

## PR Sequence (18 PRs, 6 phases)

Stacked chain: PR-3 → PR-8 → PR-9 → {PR-10, PR-11} → PR-12 → PR-13 → PR-14d. All others land independently off main (14a/14b/14c are independent production PRs that 14d soft-depends on via capability checks).

### Phase 1: Foundation and quick wins (independent)

**PR-0: land the in-flight stack first.** Get #1702 merged (it fixes harness assertions everything below relies on: dead-VIP failover passes, report artifact paths, gofmt gate). Track #1701 and #1699 to merge before the Phase 2 PRs that touch the same packages. No new work, just sequencing.

**PR-1 `ci: coverage reporting + etcd e2e suite in nightly skeleton`**
- (`fail-fast: false`, JSON reports, concurrency: already done by #1698/#1702; dropped from this PR.)
- `Makefile`: add `-coverprofile` to `unit-tests`, new `e2e-tests-etcd` target for the currently unwired `testing/e2e/etcd` suite.
- New `.github/workflows/nightly-e2e.yaml` skeleton (schedule + dispatch): etcd suite (`continue-on-error` until proven green) + coverage artifact upload.
- ~80 lines YAML/Makefile, no Go.

**PR-2 `test: reflection-based test for env/file config merge`**
- New `pkg/kubevip/config_environment_test.go`: set every settable `Config` field via its env var, assert round-trip through the merge.
- Known-missing fields (`EnableUPNP`, `EnableServiceSecurity`, `EnableEndpoints`, `EgressWithNftables`, `CleanRoutingTable`, `PerServiceElectionOnDemand`, `LoseLeadership`, `AllowInterfaceNotUp`, `Etcd.*`, `BGPConfig.Zebra`, `Mpbgp*`) go into a documented `knownUnmergedFields` allowlist so the test passes on main; each allowlist deletion becomes a trivial follow-up bugfix PR.
- ~350 lines, one file.

**PR-15 `test: remaining parallelisation / CI wall-time reduction`** (independent; can land any time in Phase 1)
- #1698 already delivered the e2e side's plumbing: configurable `GINKGO_PROCS`, `max-parallel: 3`, parallelized mode scenarios, buildx cache flags; #1702 fixes report paths. First action here is to **measure post-#1698 PR wall clock** from recent CI runs before adding anything.
- Unit: add `t.Parallel()` to independent table-driven tests in the larger suites (`pkg/lease`, `pkg/kubevip`, `pkg/debouncer`; only 16 calls exist repo-wide); add `-shuffle=on` to `make unit-tests` to flush hidden order dependencies; new Phase 2 tests written `t.Parallel()`-first.
- E2E, only if post-#1698 legs still exceed ~20 min: shard the big suites across CI matrix jobs via `--focus-file` (e.g. `arp-core` = `e2e_test.go`, `arp-ds` = `e2e_ds_test.go`), and share the image build across legs (`docker save` + artifact instead of per-leg `make dockerx86Local`, ~3-4 min per leg). `max-parallel` may need raising if legs are added.
- The new suites are parallel-safe by construction: `SecureOffset` VIP allocation already exists for concurrent specs; matrix suite shards via `MATRIX_SHARD`; fault specs that partition networks run `Serial` within their process. Metrics scraping stays per-node via docker exec, so it is unaffected by the harness metrics isolation #1702 introduced for parallel phases.
- ~100 lines Makefile/YAML + mechanical `t.Parallel()` additions. Measurable acceptance: PR e2e wall clock <=~18 min.

**PR-3 `test(e2e): metrics scraping helpers + assertions in existing suites`**
- New `testing/e2e/metrics.go`: `ScrapeMetrics(node)` via `docker exec curl -s 127.0.0.1:2112/metrics` (templates already pass `--prometheusHTTPServer`, kube-vip is hostNetwork), `GaugeValue`, `CounterDelta`, `EventuallyMetric`.
- Assert `kube_vip_is_leader`, `kube_vip_active_services`, BGP session gauge in a few existing green specs.
- Assertion style for later phases: sample gauges twice with a gap (leaked loops never decrement); assert counter deltas over a quiet window, not absolutes.
- ~250 lines.

### Phase 2: Unit-test debt in hotspot packages (independent of each other)

**PR-4 `test: unit tests for pkg/services + pkg/servicecontext lifecycle`**
- **Sequence after #1702 and #1701 merge** (both touch `pkg/services/services.go`; #1701 adds `pkg/services/services_test.go`, which this PR extends rather than creates; #1698 already added `egress_cidr_test.go` to build on).
- First broad `client-go` `fake.NewSimpleClientset` usage in repo (sets the pattern); use reactors + the existing `development.kube-vip.io/synthetic-api-server-error-on-update` hook (`pkg/services/services.go:850`).
- Targets: add/update/delete instances, status update failure paths, per-service context cancellation, duplicate-delete idempotency on leadership transition (#1561/#1553 class), and the #1702 fixes (egress CIDR kcm fallback, no-op watch events skipping the API Get). ~550 lines.

**PR-5 `test: unit tests for pkg/election + pkg/lease restart paths`**
- Lease recreation after cancelled context (#1664), election restart after loss (#1477), custom leaseName sharing, OnStoppedLeading ordering vs VIP teardown. ~400 lines.

**PR-6 `test: unit tests for pkg/cluster VIP lifecycle + pkg/instance`**
- AddIP/shutdown concurrency (#1594 deadlock shape), IP removal on leadership loss, `--preserveVipOnLeadershipLoss` branch, fake `vip.Network` implementation. ~500 lines.

**PR-7 `test: unit tests for pkg/bgp translation + endpoints providers parity`**
- **Sequence after #1699 merges** (it splits `pkg/endpoints` into per-mode files and touches providers + `pkg/manager/worker`; writing provider tests against the pre-refactor layout would conflict immediately). Coordinate with cellebyte; the per-mode split actually makes the parity tests cleaner (one test file per mode variant, plus #1699's own `endpointslices_test.go` to extend).
- BGP peer config construction, unnumbered peers, MP-BGP (+ the #1683 regular-BGP fallback path), metrics gauge set/unset on session state; EndpointSlices vs legacy Endpoints parity incl. `externalTrafficPolicy: Local` filtering; replace dead commented-out `pkg/k8s/client_test.go`. ~500 lines.
- Remaining zero-test packages (upnp, wireguard, dhcp, ndp, arp, iptables...) are tracked follow-ups, not in this sequence.

### Phase 3: Fault-injection e2e (stacked on PR-3)

**PR-8 `test(e2e): fault injection primitives (testing/e2e/faults.go)`**
- `PartitionNode`/`HealNode` (`docker network disconnect kind <node>` + reconnect; asymmetric via `iptables DROP` inside node container). `HealNode` verifies node readiness, falls back to `docker restart`.
- `BlackholeAPIServer`/`RestoreAPIServer` (iptables DROP on :6443 inside node; distinct from existing manifest-stash: node stays up, kube-vip loses API. This is the exact scenario behind the lease/watcher regressions).
- `KillKubeVip(node, signal)` (SIGKILL vs SIGTERM exit paths), `RestartNode`, `DeleteLease`/`StealLease` via k8s API.
- Deduplicate the existing `setAPIServerState` from `e2e_rt_test.go:1838` + `e2e_bgp_healthcheck_test.go` into shared `StashPodManifest`/`RestorePodManifest` (net deletion in those files).
- No chaos framework, builds entirely on existing docker-exec plumbing in `kind.go`. ~350 lines new.

**PR-9 `test(e2e): fault suite for control-plane election + VIP failover`**
- New `testing/e2e/e2e_faults_cp_test.go` with Ginkgo `Label("faults")`; introduce label filtering (`--label-filter='!faults && !scale'` in existing Makefile targets so PR CI is unchanged; new `e2e-tests-faults` target).
- Scenarios (ARP + RT via TEST_MODE): apiserver blackhole on leader then heal (lease recreated, exactly one `kube_vip_is_leader==1`), SIGKILL kube-vip on leader, lease delete/steal, node restart. Each ends with steady-state metric checks (bounded `kube_vip_leader_election_transitions_total` delta).
- Harness hardening here: set `rest.Config.QPS=50, Burst=100` for the fault-suite clientset and poll at >=1s intervals (client-go default 5 QPS turns fault-polling loops into fake convergence failures: "client rate limiter Wait returned an error").
- ~500 lines.

**PR-10 `test(e2e): fault suite for per-service election + service lifecycle`**
- New `testing/e2e/e2e_faults_svc_test.go`, `Label("faults")`. Hotspot #1: per-service and on-demand election under apiserver blackhole (watcher restart, #1511 class), leader kill during service delete (global vs per-service election), synthetic-error annotation mid-reconcile then assert `kube_vip_service_reconcile_errors_total` stops growing after heal, DNS/FQDN VIP refresh across a fault window (#1667 class), egress-nftables SNAT re-scoping on new leader (`docker exec nft list ruleset`). ~550 lines.
- Egress specs must encode the #1701/#1702 semantics: active-endpoint annotation only written by the endpoint watcher (a startup Service snapshot must not overwrite a newer endpoint), and SNAT exclusion covering ALL node pod CIDRs, not just the local node's (the #1702 cross-node SNAT bug is exactly the class an e2e fault spec should have caught: kill the leader, then assert pod-to-pod traffic across nodes is not SNAT'd to the VIP).

### Phase 4: Pairwise matrix + nightly workflow

**PR-11 `test(e2e): pairwise combination matrix generator + conformance spec`**
- New `testing/e2e/matrix/matrix.go`: typed axes Mode{arp,bgp,rt,wireguard} x Function{cp,svc,both} x Family{v4,v6,dual} x Election{global,per-service,on-demand,none} x Shape{static-pod,daemonset} x Provider{slices,endpoints} x ETP{Cluster,Local}, with curated exclusion rules for invalid combos. `matrix/matrix_test.go` unit-tests pairwise completeness + determinism.
- New `testing/e2e/e2e_matrix_test.go`: one parameterized deploy-combo/create-service/assert-VIP+metrics spec via `DescribeTable`, `Label("matrix")`, sharded via `MATRIX_SHARD=i/n`. Yields ~40-60 combos vs 1000+ cross-product. WireGuard entries may `Skip()` initially (documents the gap; it has zero e2e today).
- ~600 lines.

**PR-12 `ci: nightly workflow for fault, matrix, and etcd suites`**
- Extend `nightly-e2e.yaml`: `e2e-tests-faults` (arp/rt/bgp), matrix sharded 3-4 ways, etcd suite, service-tests. Triggers: schedule, `workflow_dispatch`, PR label `e2e-full`. `fail-fast: false`, job timeouts, log artifacts (`E2E_KEEP_LOGS` pattern), keep the `fs.inotify` sysctl bumps. ~200 lines YAML.

### Phase 5: Scale (nightly only)

**PR-13 `test(e2e): controller-behavior scale suite`**
- New `testing/e2e/e2e_scale_test.go` + `scale_helpers.go`, `Label("scale")`, one nightly job. On 1 CP + 2 worker kind:
  - 150 LB services in batches of 25 with a single shared backend deployment; measure time-to-all-advertised via `kube_vip_active_services` + spot checks; assert generous bound.
  - 10-min churn loop (~1 op/s); afterwards `kube_vip_active_services` == actual count exactly (ghost/leak detection), reconcile-error delta ~0.
  - Election churn: kill leader kube-vip 10x; all leases re-acquired, transitions bounded.
  - Endpoint fan-out: ETP Local, deployment 1→30→1 repeatedly.
- Runner guardrails documented as constants: <=3 kind nodes, <=150 services, harness QPS/Burst 100/200, poll >=1s. ~500 lines.

### Phase 6: Full-coverage observability (metrics for everything)

Goal: every subsystem exposes metrics sufficient to observe its health and activity externally, so tests (and operators) never depend on log-scraping. Existing metrics today are only: `kube_vip_active_services`, `kube_vip_service_reconcile_errors_total`, `kube_vip_service_reconcile_duration_seconds`, `kube_vip_manager_all_services_events`, `kube_vip_leader_election_transitions_total`, `kube_vip_is_leader`, `kube_vip_manager_bgp_session_info`, `kube_vip_build_info`. Production PRs kept small and strictly additive (higher upstream review bar), split by subsystem so each is independently reviewable.

**PR-14a `feat(metrics): loop-liveness gauges for watcher and election loops`**
- New gauges in `pkg/metrics/`: `kube_vip_watcher_loops{kind=service|endpoint|node|lease}`, `kube_vip_election_loops{type=...}`; increment on goroutine start, `defer`-decrement on exit, at ~15-20 boundaries in `pkg/manager/worker`, `pkg/services`, `pkg/endpoints`, `pkg/election`.
- Rationale for upstream: operator-facing debugging feature. The signature regression class (duplicate/leaked loops after leadership transitions) is invisible functionally; a gauge that must be exactly 1 per (node, kind) is the only precise guard. Strictly additive, zero behavior change. ~120 lines prod + ~100 lines unit tests.

**PR-14b `feat(metrics): dataplane operation metrics (VIP/ARP/route/DNS)`**
- `kube_vip_vip_addresses{interface,family}` gauge (VIPs currently held; the external ground truth for "who owns what"), `kube_vip_vip_operations_total{op=add|delete,result=ok|error}`, `kube_vip_arp_advertisements_total{result}` / NDP equivalent, `kube_vip_route_operations_total{op,result}` (RT mode), `kube_vip_dns_resolutions_total{result}` + `kube_vip_dns_ip_changes_total` (guards the chronic #1667/#1390 FQDN-refresh class), DHCP lease events/failures.
- Instrumentation points in `pkg/vip/address.go`, `pkg/arp`, `pkg/route`, `pkg/vip/dns.go`, `pkg/vip/dhcp*.go`. ~150 lines prod + unit tests.

**PR-14c `feat(metrics): BGP, egress/nftables, and watcher-restart metrics`**
- BGP: `kube_vip_bgp_routes_advertised{family}` gauge, `kube_vip_bgp_route_operations_total{op,result}`, peer session state gauge per peer (extends the existing session-info metric; this area's metrics broke 3x, so unit-test gauge set/unset transitions).
- Egress/nftables: `kube_vip_egress_rules{table}` gauge, `kube_vip_egress_operations_total{op,result}` (SNAT apply/delete), instance-table ownership count.
- Watchers: `kube_vip_watcher_restarts_total{kind,reason}` (guards the #1511 node-watcher-restart class), `kube_vip_upnp_mappings` gauge if UPNP enabled.
- ~150 lines prod + unit tests.

**PR-14d `test(e2e): metric-based assertions across fault, matrix, and scale suites`**
- After every fault/heal and post-churn: `EventuallyMetric` that each loop gauge equals its expected value on every node, sampled twice with a gap; `kube_vip_vip_addresses` matches the docker-exec `ip addr` ground truth (cross-validation of the metric itself); route/BGP/egress operation counters quiet in steady state; watcher-restart counter increments exactly when a fault should cause a restart and stays flat otherwise.
- Matrix suite steady-state check switches from functional-only to functional + metrics, giving every pairwise combo an internal-health assertion for free.
- Capability checks (Skip if a metric is absent) so test PRs don't hard-depend on 14a-14c merge timing. ~250 lines.

## Risks and mitigations

- **Flake on shared runners**: QPS/Burst raised in fault/scale harness clients, poll intervals >=1s, generous Eventually windows (2-3 min for heals), `fail-fast: false`, logs always uploaded.
- **Runner resources** (ubuntu-latest ~7GB/4vCPU): caps above; matrix suite reuses one cluster per mode where possible to bound nightly wall time.
- **Maintainer bandwidth**: 15/18 PRs test/CI-only; PR descriptions state "no production code changed"; PR-2 allowlist converts found bugs into one-line follow-ups instead of debates inside a test PR. The three metrics PRs are pitched as operator-facing observability features, split by subsystem.
- **Over-strict fault assertions**: counters can have benign startup increments and concurrency gauges can transiently show old+new state during a legitimate rebuild. Assert delta-stability (counter stops growing) rather than absolute zero, and settle-based polling (value stabilizes within timeout) rather than single scrapes. Validate every fault spec bidirectionally: it must pass on the fixed/current code AND fail on a known-bad commit; if the fixed branch fails, revise the assertion, not the code.
- **`docker network disconnect` on kind** can leave nodes NotReady after reconnect: `HealNode` verifies readiness with `docker restart` fallback; partition specs run last in ordered containers.
- **etcd suite rot**: best-effort in PR-1 until proven green.

## Verification

- Before any work: `git pull` to origin/main (local checkout is at d1fa3a2, ~30 commits behind incl. the #1698 testing refactor) and re-check which in-flight PRs (#1702, #1701, #1699) have merged.
- Each PR: `make unit-tests` and the relevant `make e2e-tests-*` target locally (kind + Docker; on macOS run the suite container with `--network host`), plus green PR CI.
- PR-8/9/10: deliberately verify fault helpers against a known-bad commit (e.g. check out pre-#1664 and confirm the lease-recreation spec fails) so each fault spec is proven to catch its regression class.
- PR-11: matrix generator unit test proves pairwise completeness; run one shard locally end-to-end.
- PR-12/13: trigger `workflow_dispatch` on the fork before marking ready; confirm wall time and artifact upload.
- Coverage trend visible from PR-1's artifact; expect `pkg/services`/`pkg/cluster`/`pkg/election` to move from near-0 to majority coverage after Phase 2.
