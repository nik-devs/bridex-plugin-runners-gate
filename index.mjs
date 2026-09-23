import crypto from "node:crypto";
import fs from "node:fs";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import path from "node:path";

/**
 * runners-gate: agents submit heavy operations; Cloud Run Jobs execute them
 * against a shared GCS bucket; the worker calls back with an HMAC-signed
 * result and the requesting agent is woken IN ITS ORIGINAL SESSION. A
 * watchdog polls Cloud Run for jobs that died without calling back — hook
 * for speed, polling for truth.
 *
 * Several runners (different worker images) hang off one gate; submissions
 * route by operation name, which must be unique across runners:
 *
 *   plugins:
 *     runners-gate:
 *       config:
 *         callback_base: https://bridex.example.com
 *         bucket: my-shared-bucket
 *         runners:
 *           montage:
 *             job: projects/<p>/locations/<l>/jobs/render-worker
 *             ops: [video_compose, video_stitch, subtitle_gen]
 *           convert:
 *             job: projects/<p>/locations/<l>/jobs/convert-worker
 *             ops: [pdf_to_png]
 *             bucket: optional-override
 *
 * GCP-resident by design: auth rides the VM metadata server — the instance
 * and the jobs live in one project, no key files anywhere. The only secret
 * is RUNNERS_GATE_CALLBACK_SECRET, shared with the worker images.
 */

const DEFAULT_DEADLINE_MIN = 30;
const WATCH_INTERVAL_MS = 90_000;
const LEDGER_TTL_DAYS = 7;

export default async function activate(ctx) {
  const { z, log } = ctx;
  const secret = process.env.RUNNERS_GATE_CALLBACK_SECRET ?? "";

  // -- config: the runner map ----------------------------------------------

  const cfg = ctx.config ?? {};
  const callbackBase = String(cfg.callback_base ?? "").replace(/\/+$/, "");
  const defaultBucket = String(cfg.bucket ?? "");
  /** @type {Record<string, {job: string, ops: string[], bucket?: string}>} */
  const runners = typeof cfg.runners === "object" && cfg.runners ? cfg.runners : {};

  const opToRunner = new Map();
  for (const [name, r] of Object.entries(runners)) {
    for (const op of r.ops ?? []) {
      if (opToRunner.has(op))
        throw new Error(
          `op "${op}" is declared by both "${opToRunner.get(op)}" and "${name}" — op names must be unique across runners (route by op is the whole point)`,
        );
      opToRunner.set(op, name);
    }
  }
  // probe is every worker image's built-in self-test; it routes via `runner`
  const allOps = [...opToRunner.keys()];

  if (!callbackBase || !Object.keys(runners).length || !allOps.length) {
    ctx.needsConfig("set plugins.runners-gate.config: callback_base, bucket, and runners{job, ops}");
    return;
  }

  // -- durable job ledger: survives restarts, feeds the watchdog ------------

  const stateDir = path.join(ctx.paths.state, "runners-gate");
  fs.mkdirSync(stateDir, { recursive: true });
  const ledgerFile = path.join(stateDir, "jobs.json");
  let jobs = {};
  try {
    jobs = JSON.parse(fs.readFileSync(ledgerFile, "utf8"));
  } catch {
    /* first boot */
  }
  const persist = () => fs.writeFileSync(ledgerFile, JSON.stringify(jobs, null, 2));

  // -- GCP auth via the VM's metadata server (no key files) -----------------

  async function gcpToken() {
    const res = await fetch(
      "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token",
      { headers: { "Metadata-Flavor": "Google" } },
    );
    if (!res.ok) throw new Error(`metadata token: HTTP ${res.status} (runners-gate is GCP-resident by design)`);
    return (await res.json()).access_token;
  }

  async function launchExecution(runnerName, jobId, op, args, inputs, deadlineMin) {
    const runner = runners[runnerName];
    const bucket = runner.bucket || defaultBucket;
    const spec = { job_id: jobId, op, args, inputs, bucket, callback_url: `${callbackBase}/api/v1/x/runners-gate/callback` };
    const res = await fetch(`https://run.googleapis.com/v2/${runner.job}:run`, {
      method: "POST",
      headers: { authorization: `Bearer ${await gcpToken()}`, "content-type": "application/json" },
      body: JSON.stringify({
        overrides: {
          containerOverrides: [{ env: [{ name: "RUNNER_JOB_SPEC", value: JSON.stringify(spec) }] }],
          timeout: `${deadlineMin * 60}s`,
        },
      }),
    });
    if (!res.ok) throw new Error(`Cloud Run :run HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
    const lro = await res.json();
    return lro?.metadata?.name ?? null; // execution resource — what the watchdog polls
  }

  async function executionState(execution) {
    const res = await fetch(`https://run.googleapis.com/v2/${execution}`, {
      headers: { authorization: `Bearer ${await gcpToken()}` },
    });
    if (!res.ok) return null;
    const e = await res.json();
    if ((e.succeededCount ?? 0) > 0) return "succeeded";
    if ((e.failedCount ?? 0) > 0 || (e.cancelledCount ?? 0) > 0) return "failed";
    return "running";
  }

  // -- waking the requester -------------------------------------------------

  /**
   * Last mile: the INSTANCE does not mount the bucket, so the gate pulls the
   * finished files into workspace artifacts itself — agents get paths they
   * can actually touch (and deliver with `message` media directly).
   */
  async function fetchOutputs(job, jobId) {
    const saved = [];
    const bucket = job.bucket || defaultBucket;
    for (const rel of job.outputs ?? []) {
      const dest = path.join(ctx.paths.workspaceArtifacts(job.workspace), "renders", jobId, path.basename(rel));
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      const token = await gcpToken();
      const res = await fetch(
        `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(bucket)}/o/${encodeURIComponent(rel)}?alt=media`,
        { headers: { Authorization: `Bearer ${token}` } },
      );
      if (!res.ok) throw new Error(`GCS fetch ${rel}: HTTP ${res.status}`);
      await pipeline(Readable.fromWeb(res.body), fs.createWriteStream(dest));
      saved.push(`artifacts/renders/${jobId}/${path.basename(rel)}`);
    }
    return saved;
  }

  /**
   * Way in, same last mile: inputs named `artifacts/...` are files in the
   * agent's workspace — the instance does not mount the bucket, so the gate
   * uploads them to `inbox/<jobId>/` and rewrites the spec to bucket paths.
   * Anything else passes through as an already-bucket-relative path.
   */
  async function stageInputs(workspace, jobId, bucket, inputs) {
    const staged = [];
    for (const item of inputs) {
      const rel = String(item);
      if (!rel.startsWith("artifacts/")) {
        staged.push(rel);
        continue;
      }
      const abs = path.join(ctx.paths.workspaceArtifacts(workspace), rel.slice("artifacts/".length));
      if (!fs.existsSync(abs) || !fs.statSync(abs).isFile())
        throw new Error(`input not found in workspace artifacts: ${rel}`);
      const dest = `inbox/${jobId}/${path.basename(abs)}`;
      const token = await gcpToken();
      const res = await fetch(
        `https://storage.googleapis.com/upload/storage/v1/b/${encodeURIComponent(bucket)}/o?uploadType=media&name=${encodeURIComponent(dest)}`,
        { method: "POST", headers: { Authorization: `Bearer ${token}` }, body: fs.createReadStream(abs), duplex: "half" },
      );
      if (!res.ok) throw new Error(`GCS upload ${rel}: HTTP ${res.status}`);
      staged.push(dest);
    }
    return staged;
  }

  async function finish(jobId, ok, detail) {
    const job = jobs[jobId];
    if (!job || job.status !== "pending") return; // callback and watchdog may race; first wins
    job.status = ok ? "done" : "failed";
    if (ok) job.outputs = detail.outputs ?? [];
    else job.error = detail.error ?? "unknown";
    persist();
    // the render's minutes belong to the task like any other paid tool — the
    // task page and the spend tables read them (price per minute is optional)
    try {
      const seconds = Math.max(1, Math.round((Date.now() - (job.createdAt ?? Date.now())) / 1000));
      const perMin = Number(cfg.price_per_min_usd ?? 0) || 0;
      ctx.usage?.record?.({
        workspace: job.workspace,
        agent: job.agent,
        kind: `runner:${job.runner}`,
        model: job.op,
        costUsd: (seconds / 60) * perMin,
        units: seconds,
        unit: "s",
        taskId: job.taskId ?? null,
      });
    } catch (e) {
      log.warn(`job ${jobId}: usage record failed: ${e.message}`);
    }
    let where = "";
    if (job.outputs?.length) {
      try {
        const saved = await fetchOutputs(job, jobId);
        where = `\nOutputs (already in your workspace artifacts):\n${saved.map((o) => `- ${o}`).join("\n")}`;
      } catch (e) {
        log.warn(`job ${jobId}: output download failed: ${e.message}`);
        where = `\nOutputs are in the shared bucket (download to artifacts FAILED: ${e.message}):\n${job.outputs.map((o) => `- ${o}`).join("\n")}`;
      }
    }
    ctx.wakeAgent({
      workspace: job.workspace,
      agent: job.agent,
      key: `runner:${jobId}`,
      ...(job.sessionKey ? { sessionKey: job.sessionKey } : {}),
      prompt: ok
        ? `Runner job ${jobId} (${job.op} on ${job.runner}) finished successfully.${where}\nContinue the work that requested it${job.taskId ? ` (task ${job.taskId})` : ""}.`
        : `Runner job ${jobId} (${job.op} on ${job.runner}) FAILED: ${job.error}. Decide: retry with adjusted args, or report the blocker.`,
    });
  }

  // -- tools ----------------------------------------------------------------

  ctx.registerTool({
    name: "runner_submit",
    description:
      `Submit ONE heavy operation to a Cloud Run worker and END your run — you will be woken with the result (never poll, never wait in-run). Available ops: ${allOps.join(", ")}. Pass your source files as artifacts/... paths — the gate uploads them to the worker and downloads finished outputs back into artifacts/renders/<job>/ for you. Light work stays in your own shell.`,
    schema: {
      op: z.string().describe(`operation name; one of: ${allOps.join(", ")} (or "probe" with an explicit runner)`),
      args: z.string().describe("JSON object of op arguments; reference source files by the SAME paths you list in inputs"),
      inputs: z.string().optional().describe("JSON array of input paths: artifacts/... (your workspace files — auto-uploaded) or bucket-relative paths from an earlier job"),
      runner: z.string().optional().describe(`explicit runner (${Object.keys(runners).join(", ")}) — only needed for "probe"`),
      deadline_min: z.number().optional().describe(`minutes before the watchdog declares the job dead (default ${DEFAULT_DEADLINE_MIN})`),
    },
    async handler(args, call) {
      if (!secret) return { content: [{ type: "text", text: "error: RUNNERS_GATE_CALLBACK_SECRET is not set" }] };
      const op = String(args.op ?? "");
      const runnerName = args.runner ? String(args.runner) : opToRunner.get(op);
      if (!runnerName || !runners[runnerName])
        return { content: [{ type: "text", text: `error: no runner serves op "${op}" — available: ${allOps.join(", ")}` }] };
      if (op !== "probe" && !runners[runnerName].ops.includes(op))
        return { content: [{ type: "text", text: `error: runner "${runnerName}" does not declare op "${op}"` }] };
      let parsedArgs, parsedInputs;
      try {
        parsedArgs = JSON.parse(String(args.args ?? "{}"));
        parsedInputs = args.inputs ? JSON.parse(String(args.inputs)) : [];
      } catch {
        return { content: [{ type: "text", text: "error: args/inputs must be valid JSON" }] };
      }
      const deadlineMin = Number(args.deadline_min) > 0 ? Number(args.deadline_min) : DEFAULT_DEADLINE_MIN;
      const jobId = `rj-${Date.now().toString(36)}-${crypto.randomBytes(3).toString("hex")}`;
      try {
        const jobBucket = runners[runnerName].bucket || defaultBucket;
        const stagedInputs = await stageInputs(call.workspace, jobId, jobBucket, parsedInputs);
        // args may reference the same artifacts/ paths — rewrite them in lockstep
        let argsJson = JSON.stringify(parsedArgs);
        parsedInputs.forEach((orig, i) => {
          if (String(orig).startsWith("artifacts/")) argsJson = argsJson.split(String(orig)).join(stagedInputs[i]);
        });
        parsedArgs = JSON.parse(argsJson);
        const execution = await launchExecution(runnerName, jobId, op, parsedArgs, stagedInputs, deadlineMin);
        jobs[jobId] = {
          op,
          runner: runnerName,
          bucket: runners[runnerName].bucket || defaultBucket,
          workspace: call.workspace,
          agent: call.agent,
          sessionKey: call.sessionKey,
          taskId: call.taskId,
          execution,
          status: "pending",
          deadlineAt: Date.now() + deadlineMin * 60_000,
          createdAt: Date.now(),
        };
        persist();
        log.info(`job ${jobId} (${op} → ${runnerName}) launched for @${call.agent}`);
        return {
          content: [
            {
              type: "text",
              text: `submitted: job ${jobId} (${op} → ${runnerName}). END this run now — you will be woken when it finishes (deadline ${deadlineMin} min).`,
            },
          ],
        };
      } catch (err) {
        return { content: [{ type: "text", text: `error: launch failed — ${err.message}` }] };
      }
    },
  });

  ctx.registerTool({
    name: "runner_status",
    description: "Check one runner job by id (manual reconciliation — normally the wakeup finds you first).",
    schema: { job_id: z.string() },
    async handler(args) {
      const job = jobs[String(args.job_id)];
      if (!job) return { content: [{ type: "text", text: "error: unknown job id" }] };
      return { content: [{ type: "text", text: JSON.stringify({ id: args.job_id, ...job }) }] };
    },
  });

  // -- callback route: the fast path ---------------------------------------

  const app = new ctx.Hono();
  app.post("/callback", async (c) => {
    const raw = await c.req.text();
    const sig = c.req.header("x-runner-signature") ?? "";
    const expected = crypto.createHmac("sha256", secret).update(raw).digest("hex");
    const a = Buffer.from(sig), b = Buffer.from(expected);
    if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return c.json({ error: "bad signature" }, 403);
    let body;
    try {
      body = JSON.parse(raw);
    } catch {
      return c.json({ error: "bad json" }, 400);
    }
    if (!body.job_id || !jobs[body.job_id]) return c.json({ error: "unknown job" }, 404);
    await finish(String(body.job_id), body.ok === true, { outputs: body.outputs, error: body.error });
    return c.json({ ok: true });
  });
  ctx.registerRoute(app);

  // -- watchdog: the truth path (hook AND polling, never hook alone) --------

  const timer = setInterval(() => {
    void (async () => {
      for (const [id, job] of Object.entries(jobs)) {
        if (job.status !== "pending") continue;
        try {
          const state = job.execution ? await executionState(job.execution) : null;
          if (state === "failed") return void finish(id, false, { error: "Cloud Run execution failed (no callback received)" });
          if (state === "succeeded") continue; // give the callback a beat; the deadline still backstops
        } catch (err) {
          log.warn(`watchdog poll failed for ${id}: ${err.message}`);
        }
        if (Date.now() > job.deadlineAt)
          void finish(id, false, { error: `deadline exceeded (${Math.round((job.deadlineAt - job.createdAt) / 60000)} min) — treat as hung` });
      }
      const cutoff = Date.now() - LEDGER_TTL_DAYS * 86_400_000;
      let dirty = false;
      for (const [id, job] of Object.entries(jobs))
        if (job.status !== "pending" && job.createdAt < cutoff) {
          delete jobs[id];
          dirty = true;
        }
      if (dirty) persist();
    })();
  }, WATCH_INTERVAL_MS);
  timer.unref?.();
  ctx.onShutdown(() => clearInterval(timer));

  // -- dashboard stat tile: running workers + bucket usage ------------------
  // (guarded: older hosts don't have registerStat yet)

  if (typeof ctx.registerStat === "function" && defaultBucket) {
    let bucketCache = { at: 0, gb: null };
    const bucketUsedGb = async () => {
      if (Date.now() - bucketCache.at < 300_000) return bucketCache.gb;
      const token = await gcpToken();
      let bytes = 0;
      let pageToken = "";
      // cap the walk: a bucket with >50k objects reports a lower bound
      for (let page = 0; page < 50; page++) {
        const url = `https://storage.googleapis.com/storage/v1/b/${encodeURIComponent(defaultBucket)}/o?fields=items(size),nextPageToken&maxResults=1000${pageToken ? `&pageToken=${pageToken}` : ""}`;
        const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
        if (!res.ok) throw new Error(`GCS list HTTP ${res.status}`);
        const data = await res.json();
        for (const o of data.items ?? []) bytes += Number(o.size ?? 0);
        pageToken = data.nextPageToken ?? "";
        if (!pageToken) break;
      }
      bucketCache = { at: Date.now(), gb: Math.round((bytes / 1e9) * 10) / 10 };
      return bucketCache.gb;
    };
    ctx.registerStat(async () => {
      const running = Object.values(jobs).filter((j) => j.status === "pending").length;
      let sub;
      try {
        sub = `bucket ${await bucketUsedGb()} GB used`;
      } catch {
        sub = `bucket ${defaultBucket}`;
      }
      return [{ label: "Runners", value: running, sub }];
    });
  }

  const pending = Object.values(jobs).filter((j) => j.status === "pending").length;
  log.info(
    `runners-gate active: ${Object.keys(runners).length} runner(s), ${allOps.length} op(s)${pending ? `, ${pending} pending job(s) resumed` : ""}`,
  );
}
