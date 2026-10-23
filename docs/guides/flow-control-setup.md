# Flow Control Setup for Batch and Interactive Inference

This guide describes how to configure llm-d Router flow control and Batch Gateway to efficiently support both interactive (online) and batch (offline) inference workloads on shared infrastructure.

## Goal

- Interactive inference requests get low-latency, high-priority treatment.
- Batch inference requests fill remaining capacity when the backend is underutilized.
- When backend saturation increases due to interactive traffic, batch request dispatch automatically decreases.
- When backend saturation decreases, batch request dispatch automatically increases.

## How Flow Control Works

llm-d Router's flow control is a sharded queuing and dispatch engine that sits between the EPP and model servers. When the `flowControl` feature gate is enabled, all inference requests pass through a three-tier dispatch hierarchy:

1. **Priority Band Selection** -- Requests are assigned to priority bands by numerical level. Higher-priority bands are dispatched first; lower bands are served only when higher bands are empty.
2. **Fairness Policy** -- Within a priority band, a fairness policy selects which flow (logical grouping of requests) to serve next. Options are `round-robin` (prevents starvation) or `global-strict` (maximizes throughput).
3. **Ordering Policy** -- Within a selected flow, an ordering policy picks the next request. Options are `FCFS`, `EDF` (earliest-deadline-first), or `SLO-deadline` (orders by `ReceivedTimestamp + x-slo-ttft-ms`).

A **saturation detector** monitors backend load and applies head-of-line blocking when saturation reaches 1.0 -- pausing all dispatch until capacity becomes available. Higher-priority bands resume first.

For full details, see the [llm-d Router flow-control guide](https://github.com/llm-d/llm-d-router/blob/main/pkg/epp/flowcontrol/README.md) and [flow-control architecture](https://github.com/llm-d/llm-d-router/blob/main/docs/architecture.md#flowcontrol).

## Recommended Flow Control Configuration

### Priority Band Design

|Band|Priority|Workload|Fairness|Ordering|Rationale|
|------|--------|----------------------|-------------|------------|-----------------------------------------------|
|Interactive|100|Interactive requests|round-robin|fcfs|Low latency, fair across tenants|
|Batch|-1|Batch requests|global-strict|slo-deadline|Sheddable; maximizes throughput, dispatches by SLO urgency|

**Why this works:** When the backend is not saturated, both bands dispatch freely. When saturation reaches 1.0 and head-of-line blocking activates, the priority hierarchy ensures interactive requests (priority 100) are dispatched before batch requests (priority -1). Because batch requests have negative priority, they are **sheddable**, so they yield to interactive traffic. With the `flowControl` feature gate enabled they buffer under the band's capacity limits and queue TTL and are shed when either is exhausted (HTTP 429/503); without the gate they are rejected on arrival once the pool is saturated. Batch dispatch resumes as soon as saturation drops. Interactive requests without an `InferenceObjective` header default to priority 0 and are still protected, since they outrank the -1 batch band. If batch requests are queued but saturation persists, they are evicted when their TTL expires.

**Why sheddable (negative priority) for batch:** With non-negative priority, batch requests would compete with interactive traffic for queue capacity. Negative priority keeps them at the bottom of the dispatch order and lets the batch processor absorb the backpressure it is already designed to handle. The processor sends a fresh `x-slo-ttft-ms` on each retry, so retried requests are re-prioritized correctly by SLO urgency when they're eventually admitted.

**SLO-deadline ordering for batch:** The batch band uses `slo-deadline-ordering-policy`, which orders requests by their SLO deadline (`ReceivedTimestamp + x-slo-ttft-ms`), ensuring inference requests of jobs closer to their completion deadline are dispatched first. See the [Router SLO Deadline Ordering Policy](https://github.com/llm-d/llm-d-router/blob/main/pkg/epp/framework/plugins/flowcontrol/ordering/slodeadline/README.md).

**FCFS ordering for interactive:** Interactive requests typically don't carry `x-slo-ttft-ms` headers, so `slo-deadline-ordering-policy` would assign them all a far-future deadline — effectively degrading to FCFS with extra overhead. FCFS also provides predictable arrival-order dispatch. If interactive clients do send SLO headers (e.g., latency-sensitive API tiers), consider switching the interactive band to `slo-deadline` as well.

### EndpointPickerConfig

`llm-d.ai/v1` requires llm-d-router v1.0.0 or later. On older routers use `llm-d.ai/v1alpha1`: it is
still accepted and converted internally, but logs `DEPRECATION: apiVersion
llm-d.ai/v1alpha1/EndpointPickerConfig is deprecated and is removed in a later release`. Decoding is
strict, so an `apiVersion` the router does not register fails to start the EPP.

```yaml
apiVersion: llm-d.ai/v1
kind: EndpointPickerConfig
featureGates:
  - "flowControl"

# schedulingProfiles is omitted — llm-d Router auto-populates a default profile.

plugins:
  - type: round-robin-fairness-policy
  - type: global-strict-fairness-policy
  - type: slo-deadline-ordering-policy
  - type: utilization-detector
    parameters:
      # Max pending queue depth per model server before saturation is triggered.
      queueDepthThreshold: 5
      # Max KV-cache utilization (0-1.0) before saturation is triggered.
      kvCacheUtilThreshold: 0.8

flowControl:
  # Global queue capacity across all bands and shards.
  # Size according to expected peak concurrent requests.
  maxBytes: 4294967296  # 4Gi

  # Fallback TTL for requests that don't specify x-slo-ttft-ms.
  # Interactive requests without SLO headers expire after 30 seconds.
  defaultRequestTTL: 30s

  priorityBands:
    # --- Interactive band: high priority, fair ---
    - priority: 100
      maxBytes: 1073741824  # 1Gi
      fairnessPolicyRef: round-robin-fairness-policy
      orderingPolicyRef: fcfs-ordering-policy

    # --- Batch band: sheddable, throughput-optimized ---
    - priority: -1
      maxBytes: 3221225472  # 3Gi
      fairnessPolicyRef: global-strict-fairness-policy
      orderingPolicyRef: slo-deadline-ordering-policy

  # Template for any priority values not explicitly listed above.
  defaultPriorityBand:
    maxBytes: 536870912  # 512Mi
    fairnessPolicyRef: global-strict-fairness-policy
    orderingPolicyRef: fcfs-ordering-policy

  saturationDetector:
    pluginRef: utilization-detector
```

Notes on this config:

- **`featureGates: ["flowControl"]` is required.** Flow control is off by default. Without the gate,
  everything under `flowControl:` except `saturationDetector` is ignored, with only a log warning —
  the EPP falls back to saturation-only admission.
- **Every referenced plugin must be declared.** Each `fairnessPolicyRef`, `orderingPolicyRef` and
  `saturationDetector.pluginRef` must appear in `plugins:`. Only the defaults
  (`utilization-detector`, `fcfs-ordering-policy`, `global-strict-fairness-policy`,
  `static-usage-limit-policy`) are registered automatically; a `saturationDetector` pointing at an
  undeclared plugin fails validation at startup.
- **`priorityBands` is optional.** Bands are provisioned on demand from the `InferenceObjective`
  control plane. A band that is not listed inherits `defaultPriorityBand`, or the system default
  (`global-strict` fairness, `fcfs` ordering, 1Gi / 5000 requests) when no template is given. Use
  explicit `priorityBands` when you want SLO-deadline ordering or `round-robin` fairness per band.
- **`saturationDetector` lives under `flowControl:`.** The top-level `saturationDetector:` field was
  removed in llm-d-router v0.11.0. If the selected detector also implements the scheduling `Filter`
  interface (e.g. `concurrency-detector`, `utilization-detector`) it is added to every scheduling
  profile automatically.
- **`enableEviction: true`** additionally allows in-flight, negative-priority requests to be revoked
  when higher-priority traffic is blocked by saturation. Off by default.
- On ODH, the EPP config is not a standalone object: set it inline via
  `spec.router.scheduler.config.inline` on the `LLMInferenceService`. An inline config **replaces**
  the platform's default EPPConfig preset rather than merging with it, so it must also carry the
  default scheduling plugins and the `metrics-data-source` plugin.

### How Requests Get Assigned to Bands

Flow control assigns requests to priority bands based on the `InferenceObjective` Kubernetes CRD referenced by each request. The request carries the CRD name in the `x-llm-d-inference-objective` header (`x-gateway-inference-objective` is a deprecated alias), and the Router EPP looks up the corresponding `InferenceObjective` resource to determine the priority band.

> **Use the group the EPP actually watches.** When both the `llm-d.ai` and legacy
> `inference.networking.x-k8s.io` InferenceObjective CRDs are installed, the EPP prefers `llm-d.ai`
> and **ignores** legacy resources, logging a warning. On such a cluster, objectives created in the
> legacy group are silently unused. The EPP ServiceAccount needs `get`/`list`/`watch` on
> `inferenceobjectives` in whichever group is in use.

**Setup:**

1. Create `InferenceObjective` CRDs for each workload class. The CRD (`llm-d.ai_inferenceobjectives.yaml`) comes from llm-d-router, not GAIE. `spec.poolRef` is a single reference, not a list:

```yaml
apiVersion: llm-d.ai/v1alpha2
kind: InferenceObjective
metadata:
  name: interactive-default
spec:
  priority: 100
  poolRef:
    group: inference.networking.k8s.io
    name: <your-inference-pool>

---
apiVersion: llm-d.ai/v1alpha2
kind: InferenceObjective
metadata:
  name: batch-sheddable
spec:
  priority: -1
  poolRef:
    group: inference.networking.k8s.io
    name: <your-inference-pool>
```

On clusters that only serve the legacy CRD, use `inference.networking.x-k8s.io/v1alpha2` instead.

2. Configure Batch Gateway to reference the batch objective (see [Recommended Batch Gateway Configuration](#recommended-batch-gateway-configuration) below).

3. Interactive workloads can optionally send `x-llm-d-inference-objective: interactive-default` to get priority 100. Without this header, requests default to priority 0, which still outranks the batch band (priority -1).

## Recommended Batch Gateway Configuration

### Headers Sent

Batch Gateway sets the following flow-control headers on each inference request. The `x-llm-d-*` names are canonical on llm-d-router v0.11.0+; the `x-gateway-*` / `x-slo-*` names below are deprecated aliases that are still accepted.

- **`x-slo-ttft-ms`** (alias of `x-llm-d-slo-ttft-ms`): Remaining milliseconds until the batch job's SLO deadline. The Router EPP's `slo-deadline-ordering-policy` reads this header to order batch requests by urgency within the batch priority band. Only affects ordering when the batch band uses an SLO-based ordering policy; under the default `fcfs` ordering it is ignored.
- **`x-gateway-inference-objective`** (alias of `x-llm-d-inference-objective`): Name of the `InferenceObjective` CRD that determines the priority band. Only sent when `inference_objective` is configured on the gateway (see below).
- **`x-gateway-inference-fairness-id`** (alias of `x-llm-d-inference-fairness-id`): Tenant identifier for per-tenant fairness within a priority band. Set to the job's tenant ID only when `send_fairness_header` is enabled on the processor (default `false`). The Router EPP uses this header to group requests into separate flows so that a `round-robin` fairness policy can schedule them fairly.

**Note:** The recommended batch band configuration above uses `global-strict` fairness, which ignores flow boundaries and maximizes throughput. The fairness header only has effect if operators switch the batch band's fairness policy to `round-robin`.

### Recommended Processor Settings

The following `batch-processor` configuration settings interact with flow control. They apply to
**sync dispatch**; in async dispatch mode the retry/timeout knobs on gateway entries are not used —
retries and backpressure are handled by llm-d-async (re-enqueue with backoff, honoring
`Retry-After`) and dispatch rate is governed by the async gates. See
[Async dispatch](#async-dispatch).

```yaml
# Processor concurrency settings
concurrency:
  global: 100              # Fixed ceiling for total in-flight requests
  per_endpoint: 20         # Initial and max in-flight requests per endpoint
  recovery: 5              # Max concurrent job recoveries during startup
  aimd:
    enabled: true          # Set to false to use fixed per_endpoint concurrency
    min: 5                 # AIMD floor per endpoint
    backoff_factor: 0.5    # AIMD multiplicative decrease on 429/5xx
    additive_increase: 1   # AIMD additive increase on sustained success

# Gateway client settings (per gateway or global)
request_timeout: "5m"          # Generous timeout; flow control may queue or reject requests
max_retries: 3                 # Retry on 429 (rate limit) and 5xx
initial_backoff: "2s"          # Initial retry backoff
max_backoff: "30s"             # Max retry backoff
```

#### InferenceObjective Configuration

The `inference_objective` setting controls which `InferenceObjective` CRD name is sent in the `x-gateway-inference-objective` header. Set it directly on each gateway entry — `global_inference_gateway.inference_objective` or `model_gateways.<model>.inference_objective`. When empty, the header is not sent.

**Single-pool example** (all models share one InferencePool):

```yaml
global_inference_gateway:
  url: "http://epp:8081"
  inference_objective: "batch-sheddable"
```

**Multi-pool example** (each model has its own InferencePool):

```yaml
model_gateways:
  "model-a":
    url: "http://epp-a:8081"
    inference_objective: "batch-sheddable-a"  # references pool-a
  "model-b":
    url: "http://epp-b:8081"
    inference_objective: "batch-sheddable-b"  # references pool-b
```

**Mixed example** (most models share one pool, one model has its own):

```yaml
model_gateways:
  "model-a":
    url: "http://shared-epp:8081"
    inference_objective: "batch-sheddable"
  "model-b":
    url: "http://shared-epp:8081"
    inference_objective: "batch-sheddable"
  "model-c":
    url: "http://epp-c:8081"
    inference_objective: "batch-sheddable-c"
```

#### Key Considerations

- **`request_timeout`**: With flow control enabled, requests may spend time in the Router EPP queue before reaching the backend. Set this high enough to accommodate queuing time plus inference time. 5 minutes is a reasonable starting point.
- **`max_retries` and `max_backoff`**: When the system is saturated, the Router EPP sheds batch requests: a queued request whose TTL expires gets HTTP 503 ("request timed out in queue"), and a request that finds its priority band's byte budget full gets HTTP 429. Retry backoff slows resubmission pressure, while AIMD separately lowers per-endpoint concurrency (`aimd.backoff_factor`) and later raises it gradually (`aimd.additive_increase`) as successes accumulate.
- **AIMD with flow control**: Flow control decides queueing/shedding priority in the Router EPP, and AIMD controls how aggressively the processor feeds each endpoint. Together they provide two layers of backpressure response: Router-side admission/shedding plus processor-side concurrency adaptation. Set `aimd.enabled: false` to use fixed concurrency without adaptive behavior.
- **`concurrency.global`**: A hard ceiling across all endpoints. When AIMD is enabled, per-endpoint limits self-regulate via backpressure, so the global limit mostly acts as a burst ceiling. Size it high enough to avoid being the first bottleneck (e.g., `perEndpoint × expected_endpoint_count × 2`).
- **`concurrency.perEndpoint`**: Sizing depends on backend topology. For a single vLLM instance, 10–20 is reasonable. For a Router EPP pool routing to N replicas, set it higher (e.g., `20 × N`) since the pool absorbs more concurrency. With AIMD enabled, starting high is safer — AIMD backs off quickly on 429s but recovers slowly at `+additiveIncrease` per window. Starting too low means underutilizing the backend until AIMD crawls up.
- **`aimd.min`**: The minimum concurrency sustained per endpoint under heavy backpressure — AIMD will never reduce below this value, even under sustained 429s. Too high and AIMD cannot back off enough when the backend is genuinely overloaded. Too low and a few 429s starve the endpoint, with recovery very slow at `+additiveIncrease` per window. The default of 5 ensures the processor always keeps a baseline level of requests in flight per endpoint.

#### Helm Values

Single-pool deployment:

```yaml
processor:
  config:
    concurrency:
      global: 100
      perEndpoint: 20
    globalInferenceGateway:
      url: "http://epp:8081"
      inferenceObjective: "batch-sheddable"
      requestTimeout: "5m"
      maxRetries: 3
      initialBackoff: "2s"
      maxBackoff: "30s"
```

Multi-pool deployment (per-model InferenceObjective):

```yaml
processor:
  config:
    concurrency:
      global: 100
      perEndpoint: 20
    modelGateways:
      "model-a":
        url: "http://epp-a:8081"
        inferenceObjective: "batch-sheddable-a"
        requestTimeout: "5m"
        maxRetries: 3
        initialBackoff: "2s"
        maxBackoff: "30s"
      "model-b":
        url: "http://epp-b:8081"
        inferenceObjective: "batch-sheddable-b"
        requestTimeout: "5m"
        maxRetries: 3
        initialBackoff: "2s"
        maxBackoff: "30s"
```

### Async dispatch

With async dispatch the processor writes requests to Redis and the llm-d-async workers send them
to the gateway. The flow-control headers are stamped by the processor before the request is queued
and travel with it, so no extra configuration is needed beyond setting `inference_objective` on the
model entry:

```yaml
processor:
  config:
    dispatchMode: async
    modelGateways:
      "model-a":
        inferencePoolName: "pool-a"
        inferenceObjective: "batch-sheddable-a"
    asyncDispatch:
      models:
        "model-a":
          inferencePoolName: "pool-a"
          inferenceObjective: "batch-sheddable-a"
```

Only `inferencePoolName`, `inferenceObjective`, `requestQueueName` and `resultQueueName` are used per
model in async mode; `request_timeout`, `max_retries` and the backoff settings are ignored. Shed
responses (429/503) are re-enqueued by llm-d-async with backoff, and the per-queue dispatch gate
decides how much to feed the gateway — for example the `prometheus-budget` gate reads the router's
`llm_d_epp_flow_control_*` metrics as its primary source and falls back to a configured value when
they are unavailable.

On ODH, these settings come from the `LLMBatchGateway` CR rather than Helm values:
`spec.processor.dispatchMode`, `spec.processor.modelGateways.<model>.inferenceObjective` and
`spec.processor.asyncConfig`. Do not edit the batch-gateway Deployments or ConfigMaps directly.

## How the System Behaves Under Load

### Scenario 1: Backend Idle (No Interactive Traffic)

1. Saturation detector reports low utilization.
2. Flow control dispatches from the interactive band (empty) then the batch band.
3. Batch requests flow at full capacity, limited only by the processor's `concurrency.global`.
4. Batch jobs make maximum progress toward their SLO deadlines.

### Scenario 2: Interactive Traffic Arrives

1. Interactive requests enter the priority-100 band.
2. Flow control dispatches interactive requests first (strict priority).
3. Saturation detector detects increasing backend utilization.
4. When saturation reaches 1.0, head-of-line blocking pauses ALL dispatch (both bands).
5. As requests complete and saturation drops below 1.0, dispatch resumes.
6. Interactive band is served first again; batch band gets remaining capacity.

### Scenario 3: Interactive Traffic Surge (Full Saturation)

1. Backend is fully saturated by interactive traffic.
2. Flow control's head-of-line blocking pauses all dispatch.
3. With the `flowControl` gate enabled, batch requests buffer in the batch band under the queue TTL and capacity limits rather than being rejected on arrival. Requests are shed when their TTL expires (HTTP 503, queue timeout) or when the band's `maxRequests`/`maxBytes` is exhausted (HTTP 429). Without the gate, saturation alone rejects them immediately.
4. Batch Gateway receives the shed responses and retries with exponential backoff.
5. Any batch requests already queued before saturation hit are evicted if their TTL expires; with `enableEviction: true`, queued and in-flight negative-priority requests may also be revoked to reclaim capacity for higher-priority traffic.
6. When interactive traffic subsides and saturation drops, batch dispatch resumes.

### Scenario 4: Batch Job Near SLO Deadline

1. Batch Gateway sends requests with decreasing `x-slo-ttft-ms` values as the deadline approaches.
2. SLO-deadline ordering policy promotes these requests to the front of the batch queue.
3. These urgent batch requests are dispatched ahead of newer batch requests with more remaining time.
4. If the SLO deadline passes before dispatch, the processor checks the deadline and skips the request without sending it to the backend.

## Monitoring

Key metrics to watch when running batch and interactive workloads together:

| Metric | Source | What to Watch |
|--------|--------|---------------|
| `llm_d_epp_flow_control_pool_saturation` | llm-d Router | Should hover below 1.0 during mixed workloads |
| `llm_d_epp_flow_control_queue_size` | llm-d Router | Batch band queue growing = saturation approaching; at 1.0, new batch requests are shed instead of queued |
| `llm_d_epp_flow_control_request_queue_duration_seconds` | llm-d Router | High queue time in batch band = sustained saturation |
| Request evictions (TTL) | llm-d Router flow control | Batch evictions = SLO deadlines being missed |
| 503/429 response rate | Batch Gateway metrics | High 503 (queue TTL) or 429 (band capacity) rate = flow control is shedding batch |
| Batch job completion rate | Batch Gateway metrics | Should meet SLO deadlines under normal load |
| `llm_d_epp_flow_control_requests_total` | llm-d Router | Labels `priority` and `outcome` (`Dispatched`, `Rejected`, `Evicted`, …). `{priority="-1"}` confirms batch traffic is landing in the sheddable band |

> **Metric naming:** Router v0.8.0 and earlier used the legacy
> `inference_extension_*` names. Router v0.9.0-v0.10.x also emits the current signals under
> `llm_d_epp_flow_control_pool_saturation`,
> `llm_d_epp_flow_control_queue_size`, and
> `llm_d_epp_flow_control_request_queue_duration_seconds`. Router v0.11.0+
> emits only the `llm_d_epp_*` names. Flow-control metrics are labelled by `priority`
> (`pool_saturation` by `stage`: `decode`, `prefill`, `effective`).
>
> On ODH the EPP metrics endpoint requires ServiceAccount token authentication
> (`authorization.credentials_file` in the Prometheus scrape config).

## Summary

The combination of llm-d Router flow control and Batch Gateway provides automatic, infrastructure-level workload balancing:

- **llm-d Router flow control** handles admission, queuing, and dispatch ordering based on priority and SLO deadlines.
- **Batch Gateway** communicates priority via `x-gateway-inference-objective`, urgency via `x-slo-ttft-ms`, and handles backpressure via retries.
- **Priority bands** ensure interactive traffic always takes precedence.
- **SLO-deadline ordering**, when configured on the batch band, serves the most urgent batch requests first.
- **Saturation detection** automatically throttles all dispatch when the backend is overloaded.

The system is self-regulating: batch throughput scales up and down with available backend capacity.
