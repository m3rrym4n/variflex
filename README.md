<img width="1536" height="1024" alt="ChatGPT Image Aug 10, 2026, 01_29_08 PM" src="https://github.com/user-attachments/assets/22c04ebb-7591-4c45-bf90-92fb9812351f" />

# Variflex

> **⚠️ Archived.** This repository is no longer maintained on GitHub. Development has moved to a private, self-hosted Forgejo instance. This snapshot is kept for historical reference only — issues, pull requests, and pushes here are no longer monitored or accepted.

Variflex is a self-hosted orchestrator that dispatches GitHub issues to isolated, ephemeral AI coding-agent runners (Codex today) and reports structured results back. It's built for sustained, unattended runs — not just one-off dispatches — which means it treats things like rate limits, restarts, and partial failures as first-class cases rather than edge cases to work around later.

## Why this exists

A survey of the existing open-source AI coding agent orchestrator landscape — [Bernstein](https://github.com/chernistry/bernstein), [OpenHands](https://github.com/All-Hands-AI/OpenHands), [Microsoft Conductor](https://github.com/microsoft/conductor), and others — found that none of them document handling coding-agent rate-limit exhaustion as a first-class case. In practice, that's the first thing that actually happens on any sustained run: an agent burns through its quota mid-task, and most tooling either loses the work, requires a manual restart, or has no structured way to know when it's safe to resume.

Variflex exists to solve that specific gap, and everything else follows from it:

- **Per-repository FIFO queues** — one task container per repository at a time, with a configurable global container cap across all repositories. Shown in the diagram above as the three colored queues feeding into the dispatcher.
- **Quota-exhaustion detection and resume** — a structured rate-limit response with an ISO 8601 reset time returns the interrupted task to the head of its queue and automatically resumes the *same session* once the quota clock allows. No lost work, no manual restart.
- **Session-durable queues** — pending order, halt state, and the active task reference all survive an orchestrator restart. A crash or redeploy doesn't lose track of what was running or what's next.
- **Human-controlled promotion** — every dispatch traces back to a GitHub issue. Work lands on `dev` automatically; `main`/stable only moves forward on an explicit human decision. Dev and main run as isolated environments, never sharing state.
- **Ephemeral, isolated runners** — every admitted task gets a freshly created runner container (via Dockhand) sharing only a persistent Codex auth volume, then is torn down after the task completes, fails, times out, or hits quota. No long-lived runner accumulating state or drift between tasks.

## Repository layout

- [`orchestrator/`](orchestrator/) — FastAPI/SQLite dispatch, queue, monitoring, and reporting service. Exposes the MCP tools used to actually drive dispatch (`run_task`, `list_tasks`, `clear_runner_halt`, `cancel_queued_task`) and the REST endpoints (`/api/tasks`, `/api/queues`, `/api/repos`) used by internal tooling and dashboards.
- [`runner/`](runner/) — independently built Codex execution shim used by dispatched containers. Implements the runner contract (`/execute`, `/resume`, `/status/{id}`, `/result/{id}`) the orchestrator dispatches against.

Each directory owns its Dockerfile, dependencies, tests, and build context. Changes are filtered by path in CI so one image does not rebuild when only the other component changes.

**One product, one always-on container, with ancillary containers spawned on demand.** `orchestrator/` and `runner/` build two separate images, but they don't run as two persistent containers. Only `variflex` (the orchestrator) runs continuously. `variflex-runner` instances are ephemeral — Dockhand creates one per admitted task and tears it down when that task finishes, fails, times out, or hits quota. During idle periods, zero runner containers exist; under load, several can run concurrently up to `TASK_RUNNER_MAX_CONCURRENT_CONTAINERS`. Think of it as one product with a single durable service plus on-demand workload containers, not two peer services that are both always running.

## Building

The orchestrator is the always-on service and the one most people building this repo actually need:

```bash
docker build \
  --build-arg "TASK_RUNNER_SOURCE_SHA=$(git rev-parse --short=7 HEAD)" \
  -t variflex:latest .
```

Test image:

```bash
docker build -t variflex:test -f Dockerfile.test . && docker run --rm variflex:test
```

Full run instructions — required environment variables, the repository registry, scheduled tasks, Ops Image checks, and the real production compose file — are in [`orchestrator/README.md`](orchestrator/README.md).

Building and running the runner (`runner/`) is a separate, ancillary concern — normally handled by Dockhand spawning ephemeral containers on demand, not something built directly as part of standing up the orchestrator. Its build/run steps, including the one-time Codex device-auth setup, are documented in [`orchestrator/README.md`'s "Codex runner" section](orchestrator/README.md#codex-runner). Runner-side per-container configuration (model selection, MCP server registration) is in [`runner/README.md`](runner/README.md).

## Compatibility policy

The product, repository, images, and containers use the Variflex name. Existing `TASK_RUNNER_*` orchestrator environment variables and `CODEX_RUNNER_*` runner variables remain supported unchanged to avoid breaking deployed configuration. They are compatibility API names, not separate product names; a future removal would require an explicit migration issue.

See the component READMEs for configuration and build instructions.
