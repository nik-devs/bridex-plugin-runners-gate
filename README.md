# bridex-plugin-runners-gate

The gate between Bridex agents and heavy compute: an agent submits ONE
operation, a **Cloud Run Job** executes it against a shared GCS bucket, and
the agent is woken **in its original session** with the result. HMAC-signed
callback for speed, execution polling for truth — a worker that dies without
calling back still resolves (failure wakeup on execution state or deadline).

The gate is deliberately dumb: it does not know what any operation means.
Worker images own the semantics; the gate owns submission, routing,
callbacks, the watchdog and the durable job ledger (survives restarts).

## Install

From the Bridex plugin catalog (Settings → Plugins), or copy this directory
to `$BRIDEX_HOME/plugins/runners-gate/` and restart.

`kind: integration` is load-bearing: the gate needs HTTP routes and agent
wakeups, which only in-process plugins have. Do not change it to `tool`.

## Configure

```yaml
plugins:
  runners-gate:
    config:
      callback_base: https://your-instance.example.com   # public URL of this instance
      bucket: your-shared-bucket                          # GCS, same region as the jobs
      runners:
        montage:
          job: projects/<project>/locations/<region>/jobs/render-worker
          ops: [video_compose, video_stitch, subtitle_gen]
        convert:
          job: projects/<project>/locations/<region>/jobs/convert-worker
          ops: [pdf_to_png]
          bucket: optional-per-runner-override
```

Plus one env var (Settings → the plugin card shows `needs env` until set):

- `RUNNERS_GATE_CALLBACK_SECRET` — shared HMAC secret; set the same value as
  `RUNNER_CALLBACK_SECRET` on every worker job template.

Op names must be unique across runners — submissions route by op, agents
never need to know runners exist. `probe` (every worker's built-in
self-test) is the one op that takes an explicit `runner` argument.

## Dashboard tile

On hosts that support `ctx.registerStat` (bridex ≥ the 2026-09 image) the gate
contributes a "Runners" tile to the dashboard home: currently running jobs
and the shared bucket's used space (GCS listing, cached 5 min; buckets over
50k objects report a lower bound).

## Worker contract

Per execution the job receives env `RUNNER_JOB_SPEC` (JSON):

```json
{ "job_id": "rj-…", "op": "video_stitch", "args": {…}, "inputs": ["path", …],
  "bucket": "…", "callback_url": "https://…/api/v1/x/runners-gate/callback" }
```

The worker reads inputs from the bucket (mounted at `/data` on Cloud Run),
works in local tmp, copies whole files back, then POSTs to `callback_url`:

```json
{ "job_id": "rj-…", "ok": true, "outputs": ["renders/rj-…/out.mp4"] }
{ "job_id": "rj-…", "ok": false, "error": "…" }
```

signed with header `x-runner-signature: hex(hmac_sha256(secret, raw_body))`.
See `bridex-runner-base` for a ready-made worker harness implementing this
contract.

## GCP-resident by design

Auth to Cloud Run rides the instance VM's metadata server — instance and
jobs live in one GCP project, no key files anywhere. The instance VM's
service account needs `run.jobs.run` + `run.executions.get` on the jobs.
