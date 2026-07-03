# Step Functions Orchestration Handoff

The full pipeline is now chained by a Step Functions state machine:

```
Ingestion ──► Normalization (Parallel) ──► Analytics ──► Delivery
                ├─ NormalizationHN
                └─ NormalizationX
   │                │                        │              │
   └──── Catch ─────┴──────── Catch ─────────┴──── Catch ───┘
                              │
                        NotifyFailure (notification Lambda, rich detail)
                              │
                        PipelineFailed (Fail)
```

## What changed

| Path | What |
|---|---|
| `modules/orchestration/main.tf` | State machine + its IAM role (`lambda:InvokeFunction` only), EventBridge role + retarget (cron now starts the state machine, not ingestion directly), SNS/alarms kept as-is |
| `modules/orchestration/templates/pipeline.asl.json` | The state machine definition (ASL), Lambda ARNs injected via `templatefile()` |
| `modules/orchestration/variables.tf` / `outputs.tf` | ARNs for all pipeline Lambdas in; `pipeline_state_machine_arn` out |
| root `main.tf` / `outputs.tf` | Wiring + state machine ARN output |
| `src/notification/lambda_function.py` | Accepts direct invocation from the state machine (previously SNS-only) and formats the Step Functions failure shape with stack traces |

**No pipeline Lambda code changed.** Ingestion already returns the date it
resolved, and every Lambda already no-ops gracefully on missing data —
which is exactly what the design leans on (below).

## How the date flows

The state machine passes its raw execution input to `ingestion`
(`"Payload.$": "$"`). Ingestion resolves the date (from the input, or
yesterday UTC) and **returns it**; the state machine extracts it
(`ResultSelector` → `$.ingestion.date`) and passes exactly that value to
both normalizations, analytics, and delivery. Consequences:

- All steps are guaranteed to operate on the **same** day, even if the
  execution crosses midnight.
- `normalization_x`'s "no default date" contract is always satisfied.
- Manual runs backfill one date end-to-end:

  ```bash
  aws stepfunctions start-execution \
    --state-machine-arn $(terraform output -raw pipeline_state_machine_arn) \
    --input '{"date": "2026-07-01"}'
  ```

- The EventBridge cron (unchanged schedule, 02:00 UTC) starts the state
  machine with a clean `{}` → yesterday, same behavior as before.

## No-data behavior (daily X runs)

On a normal daily run there is no X Bronze data for yesterday. Nothing
fails: `normalization_x` reports the day in `skipped_partitions`,
`normalization_hn` returns `no_success_marker` if ingestion found nothing,
analytics skips empty writes, delivery skips empty tables. A no-data day is
a successful execution whose step outputs say "nothing to do" — visible in
the execution history, never a red X. The pipeline deliberately does **not**
short-circuit after an ingestion `no_data` result, because a manually
requested historical date can have X data even when HN has none.

## How notifications changed

Before: VPC Lambda errors only surfaced as generic CloudWatch alarms
("Threshold Crossed", no stack trace) — SNS is unreachable from inside the
VPC, so the `notify_on_error` decorator had been removed.

Now: every task state has a `Catch`. Step Functions receives the failing
Lambda's full `errorType`/`errorMessage`/`stackTrace` natively in
`$.Cause` — no SNS, no VPC networking involved — and the `NotifyFailure`
task invokes the notification Lambda **directly** with it. Discord gets a
red embed: which step failed, the error type/message, execution name, and
the stack trace. Infrastructure-level failures (`States.Timeout`,
Lambda throttling) are formatted too, just without a stack trace.
`NotifyFailure` has its own Catch, so even if Discord is down the execution
still terminates in the `PipelineFailed` state rather than hanging.

The per-Lambda CloudWatch alarms are **kept** as a backstop — they also
cover manual out-of-band invocations and error modes code can't report
(timeout, OOM). Tradeoff: a Lambda failing *inside* a pipeline execution
now produces **two** Discord messages (one generic alarm, one rich). If
that's too noisy, remove the pipeline Lambdas from the alarm `for_each`
in `modules/orchestration/main.tf` and rely on the state machine alone.

Two failure modes intentionally do **not** fail the pipeline: analytics'
per-metric errors and delivery's per-table errors (both are caught
internally, reported in the step's output, and execution continues). Only
whole-Lambda failures trip the Catch. If stricter behavior is ever wanted,
make those Lambdas re-raise when their `errors` dict is non-empty.

## Retries

Each task retries only *transient AWS-side* errors
(`Lambda.ServiceException`, `Lambda.TooManyRequestsException`, ...) with
backoff. Function errors (bugs) are deliberately **not** retried — a code
bug retried 3× would just fire three alarms and waste 15 minutes before
notifying.

## Notes

- If one Parallel branch fails, Step Functions cancels the other and fails
  the Parallel state — acceptable here since both branches are idempotent
  (re-running a day overwrites deterministically).
- X `full_scan` and delivery `{"full_refresh": true}` remain manual
  one-off Lambda invokes outside the state machine.
- LocalStack Community supports basic Step Functions, so `tflocal` applies
  still work; EC2 remains the only skipped part (`enable_ec2 = false`).
- Step Functions execution history in the console now doubles as the
  pipeline's run log: each step's input/output (dates, summaries,
  skipped partitions) is recorded per execution.
