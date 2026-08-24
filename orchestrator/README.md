# Pacific Shift Variflex

FastAPI/SQLite orchestrator that dispatches GitHub issues to registry-configured runner HTTP shims and exposes four MCP tools.

## Why this exists

Most open-source AI coding agent orchestrators — [Bernstein](https://github.com/chernistry/bernstein), [OpenHands](https://github.com/All-Hands-AI/OpenHands), [Microsoft Conductor](https://github.com/microsoft/conductor), and others surveyed in this space — don't document handling coding-agent rate limits as a first-class case. In practice, that's the first thing that actually happens on any sustained run.

Variflex exists to survive that. It's a self-hosted orchestrator for AI coding agents (Codex today) built around:

- **Per-repository FIFO queues** — one task container per repository at a time, with a configurable global container cap.
- **Quota-exhaustion detection and resume** — a structured rate-limit response with an ISO 8601 reset time returns the interrupted task to the head of its queue and automatically resumes the same session once the quota clock allows, instead of losing the work or requiring a manual restart.
- **Session-durable queues** — pending order, halt state, and the active task reference all survive an orchestrator restart.
- **Human-controlled promotion** — every dispatch traces back to a GitHub issue; `dev` builds automatically, `main`/stable only moves on explicit human action.

## Configuration

| Variable | Default | Purpose |
|---|---:|---|
| `TASK_RUNNER_RUNNERS` | `{}` | JSON mapping runner names to internal URLs, e.g. `{"codex":"http://192.168.1.68:7000"}` |
| `TASK_RUNNER_SCHEDULED_TASKS` | `[]` | JSON array of scheduled issue dispatches |
| `TASK_RUNNER_OPS_IMAGE_CHECKS` | `[]` | JSON array of scheduled operational image version-drift checks and rebuild jobs |
| `TASK_RUNNER_REPOS` | `[]` | JSON array of onboarded repository and dev/main deploy-target objects |
| `TASK_RUNNER_DATABASE` | `/data/tasks.db` | SQLite database path |
| `TASK_RUNNER_TIMEOUT_SECONDS` | `600` | Hard orchestration timeout |
| `TASK_RUNNER_OUTPUT_CAP_BYTES` | `1000000` | Maximum retained runner log size |
| `TASK_RUNNER_POLL_INTERVAL_SECONDS` | `2` | Runner status polling interval |
| `GITHUB_TOKEN` | unset | Optional token for private repositories or higher API limits |
| `TASK_RUNNER_DOCKHAND_URL` | unset | Dockhand REST API base URL for internal container deploy operations |
| `TASK_RUNNER_DOCKHAND_TOKEN` | unset | Dedicated Variflex Dockhand API token (`dh_...`); do not reuse `dockhand-mcp` credentials |
| `TASK_RUNNER_DOCKHAND_ENV` | unset | Optional Dockhand environment ID for container deploy operations |
| `TASK_RUNNER_DOCKHAND_VERIFY_TIMEOUT_SECONDS` | `60` | Maximum time to wait for a started container to verify as running or healthy |
| `TASK_RUNNER_DOCKHAND_VERIFY_INTERVAL_SECONDS` | `2` | Poll interval while verifying a started container |
| `TASK_RUNNER_MAX_CONCURRENT_CONTAINERS` | `3` | Global ceiling for simultaneously active per-task runner containers |
| `TASK_RUNNER_RUNNER_IMAGE` | `codex-runner:latest` | Image used for ephemeral runner containers |
| `TASK_RUNNER_RUNNER_AUTH_VOLUME` | `variflex-runner-auth` | Shared Codex auth/session volume mounted into each runner |
| `TASK_RUNNER_RUNNER_PORT` | `7000` | Runner shim port inside the ephemeral container |
| `TASK_RUNNER_RUNNER_NETWORK` | `bridge` | Docker network mode supplied to Dockhand at container creation |
| `TASK_RUNNER_SOURCE_SHA` | `unknown` | Source revision baked into the Variflex image and used in Ops Images tags |

The MCP Streamable HTTP endpoint is `/mcp/`; the health endpoint is `/`.

### Repository registry

Every dispatched repository must appear in the live repository registry. Each entry
selects one configured runner and records both its automatically deployed `dev`
target and human-promoted `main` target. Targets require `container`, `volume`,
and a positive integer `port`. Optional `health_path` and `expected_content`
values override the reusable workflow's generic HTTP check. `health_path`
defaults to `/` in that workflow when omitted.
Repositories may also set optional `model` and `mcp_servers` values. Variflex
passes those into each spawned container as `CODEX_RUNNER_MODEL` and
`CODEX_RUNNER_MCP_SERVERS`; runner-side consumption is tracked by issue #86.
The optional `host` field accepts `github` (the default) or `forgejo`. Forgejo
entries must also set `host_base_url` to the non-empty base URL of their Forgejo
instance. GitHub entries must not set `host_base_url`. These fields are
validated registry metadata only; dispatch does not consume them yet.

The registry is stored in Variflex's SQLite database. `TASK_RUNNER_REPOS` is an
optional seed used only when that database has no registry row yet. The checked-in
[`deploy/repos.json`](deploy/repos.json) can bootstrap a fresh database, for example:

```bash
TASK_RUNNER_REPOS="$(tr -d '\n' < deploy/repos.json)"
export TASK_RUNNER_REPOS
```

The fantasy-football repositories use the canonical runner with their
lower-cost model and `ff-mcp`/`nfl-mcp` endpoints set per repository. Their
sentinel deploy targets preserve the existing no-deploy behavior: those tool
services are operated independently and task dispatch must not replace them.

`GET /api/repos` exposes the current database-backed values to internal pipeline
and dashboard consumers. `PUT /api/repos` accepts a complete JSON array, validates
it with the same rules as the seed, and atomically replaces the registry. Changes
take effect for dispatch immediately without restarting Variflex. Dispatch rejects missing repositories and runner
mismatches before creating a task row. `pacific-shift-mcp-proxy` is deliberately
absent because its Home Assistant add-on deployment does not use this container
target model.

Dockhand configuration is an internal Variflex capability for ephemeral runner lifecycle and Ops Images
deploy steps. It is not exposed as an MCP tool. The token must be supplied at
runtime through `TASK_RUNNER_DOCKHAND_TOKEN` and should be generated under a
dedicated Variflex account.

### Scheduled tasks

Issue scheduled tasks reuse the same dispatch path as the `run_task` MCP tool. When a
configured interval fires, Variflex creates a normal task row for the target
repository issue and runner. The fire is visible in container logs and in
`list_tasks`.

Configure schedules with `TASK_RUNNER_SCHEDULED_TASKS`:

```json
[
  {
    "name": "daily-codex-health-check",
    "repo": "m3rrym4n/variflex",
    "issue_number": 15,
    "runner": "codex",
    "interval": "1d"
  }
]
```

`interval` accepts a positive number of seconds or a string with one of these
suffixes:

| Suffix | Meaning | Example |
|---|---:|---|
| `s` | seconds | `120s` |
| `m` | minutes | `2m` |
| `h` | hours | `6h` |
| `d` | days | `1d` |

To add a scheduled job, add an object to `TASK_RUNNER_SCHEDULED_TASKS` and
restart the container. To remove a scheduled job, remove its object from the
array and restart the container. The `runner` value must match a key in
`TASK_RUNNER_RUNNERS`.

Ops Image checks use the same scheduler and the same per-runner queue as normal
issue dispatches. They call the configured runner's version endpoint and, when
drift is detected, create a normal task row tied to a fixed trace issue. The
queued internal job builds, pushes, prunes, deploys, and verifies the rebuilt
Codex runner image. The fixed trace issue preserves the "written issue behind
every action" rule without creating a new GitHub issue for every maintenance
cycle.

Configure Ops Image checks with `TASK_RUNNER_OPS_IMAGE_CHECKS`:

```json
[
  {
    "name": "daily-codex-runner-rebuild-check",
    "runner": "codex",
    "repo": "m3rrym4n/variflex",
    "issue_number": 35,
    "registry": "zot.lan:5000",
    "repository": "codex-runner",
    "stop_container": "codex-runner",
    "start_container": "codex-runner",
    "auth_volume": "variflex-runner-auth",
    "buildkit_addr": "unix:///run/buildkit/buildkitd.sock",
    "source_sha": "abc1234",
    "keep_tags": 2,
    "insecure_tls": false,
    "interval": "1d"
  }
]
```

The Codex runner exposes `GET /codex/version`, returning installed version,
latest npm version, and a `drift_detected` boolean. A drift result enqueues an
internal rebuild job behind any active `codex` work. The job invokes `buildctl`
through the mounted BuildKit socket after shallow-cloning the current `main`
branch of `m3rrym4n/variflex`, builds its `runner/` source with
`CODEX_VERSION=<target>`, tags the image as
`<registry>/<repository>:<codex-version>-<repo-short-sha>`, pushes it to Zot,
runs `scripts/prune_zot_image_tags.py` to keep current plus N-1, snapshots the
running container, and replaces it through Dockhand using that inspected
configuration with the newly built image reference. It independently verifies
the replacement's running state and image, then verifies that
`variflex-runner-auth` is still mounted after the swap.
If replacement or post-deploy volume verification fails, the job recreates the
previous image and configuration from the snapshot and independently verifies
that the restored container is running. This recreate step is required: merely
stopping and starting the existing container cannot change its image reference.

The Variflex container must mount the host BuildKit socket directory at the
same in-container path used by the CrateSpy runner:

```yaml
group_add:
  - "0"
volumes:
  - /DATA/AppData/buildkit/socket:/run/buildkit
```

The BuildKit socket is group-readable by GID `0`; `group_add` lets the
non-root Variflex process open the socket without changing the container's
primary user.

The production compose file is `task-runner-compose.yaml` at the repository
root. It is gitignored (it holds real credentials — `GITHUB_TOKEN`,
`TASK_RUNNER_DOCKHAND_TOKEN`) and is the actual, live source of truth for the
running container's configuration; there is no example compose file checked
into the repository. Manual deploys should edit that file directly rather than
rebuild the `docker run` invocation below from scratch.

## Runner contract

Required endpoints are `POST /execute`, `POST /resume`, `GET /status/{execution_id}`, and `GET /result/{execution_id}`. `POST /resume` accepts the original execution request plus a persisted `session_id`; the Codex runner invokes `codex exec resume` and logs an explicit marker before falling back to a fresh dispatch if resume fails. On timeout the orchestrator also attempts `DELETE /execute/{execution_id}`. Runners should implement that optional endpoint to guarantee remote process termination; otherwise the task is still recorded as `timeout`, with the failed cancellation noted.

## Repository queues and ephemeral runners

`run_task` places every issue dispatch into a SQLite-backed FIFO queue keyed by
repository and returns a receipt with `task_id`, `status`, `position`,
`queue_length`, and `runner`. A single scheduler admits the oldest eligible
repository queue head while enforcing the configured global container cap.
Each admitted task gets a fresh Dockhand-created runner with the shared auth
volume. It is stopped and removed after success, failure, timeout, or quota
exhaustion. A failure halts only that repository's queue.
If the runner instead reports `quota_exceeded` from a structured rate-limit
event with a session ID and ISO 8601 `resets_at` timestamp, the interrupted task
returns to the head of the queue and the queue enters a distinct quota halt.
Receipts for work added during that halt include `resumes_at`. At that time a
new container resumes the same task row and Codex session before later pending work starts. Quota
responses without a usable structured reset timestamp remain generic halts.
Phrasing-only quota detections deliberately do not auto-resume: Codex's relative
duration text is neither ISO-compatible nor sufficiently reliable to schedule
unattended work.
Queues are independent per repository and survive orchestrator restarts, including
pending order, halt details, quota resume time, and the active task reference.
At startup an active task with a runner execution ID resumes monitoring that
same remote execution. If no execution ID was persisted, its remote state is
unknowable: the task is marked failed and the queue halts for operator review,
preventing a potentially duplicate dispatch. This is reconciliation, not retry.
Use the `clear_runner_halt` tool with a repository name to clear its halt and resume its
remaining pending items without retrying the failed item. Use
`cancel_queued_task` to remove and mark one still-pending item as `cancelled`;
active tasks must instead use the runner shim's execution-cancellation endpoint.

## Docker

```bash
docker build \
  --build-arg "TASK_RUNNER_SOURCE_SHA=$(git rev-parse --short=7 HEAD)" \
  -t variflex:latest .

docker stop variflex
docker rm variflex

docker run -d \
  --name variflex \
  --restart unless-stopped \
  --group-add 0 \
  -p 6002:6002 \
  -v variflex-data:/data \
  -v /DATA/AppData/buildkit/socket:/run/buildkit \
  -e 'TASK_RUNNER_RUNNERS={"codex":"http://192.168.1.68:7000"}' \
  -e 'TASK_RUNNER_SCHEDULED_TASKS=[]' \
  -e 'TASK_RUNNER_OPS_IMAGE_CHECKS=[]' \
  -e "TASK_RUNNER_REPOS=$(tr -d '\n' < deploy/repos.json)" \
  -e 'TASK_RUNNER_TIMEOUT_SECONDS=3600' \
  -e 'TASK_RUNNER_DOCKHAND_URL=http://192.168.1.68:3003' \
  -e 'TASK_RUNNER_DOCKHAND_TOKEN=<redacted>' \
  -e 'TASK_RUNNER_DOCKHAND_ENV=1' \
  -e 'GITHUB_TOKEN=<redacted>' \
  variflex:latest
```

This command is illustrative of every variable actually in use — prefer
editing `task-runner-compose.yaml` directly (see above) for real deploys, since
it already holds the live credential values and stays in sync with what's
actually running.

Supply `GITHUB_TOKEN` at runtime; do not store the token in the repository.

Run tests in Docker:

```bash
docker build -t variflex:test -f Dockerfile.test .
docker run --rm variflex:test
```

Verified via automated end-to-end dispatch.

## CI/CD

The manually dispatched `.github/workflows/dev-build-deploy.yml` calls the
reusable workflow in `pacific-shift-ci`. It builds through the shared BuildKit
daemon, pushes immutable and rolling development tags to Zot, and replaces the
existing `variflex` container through Dockhand. Deployment
uses the running container's inspected configuration as its template and
changes only the image. Generic running/image/HTTP verification and automatic
rollback are supplied by the shared workflow.

The repository requires a self-hosted runner labeled `zimaos` and
`variflex`, plus `DOCKHAND_URL` and `DOCKHAND_TOKEN` Actions secrets, before
the workflow can be dispatched. Runner and token provisioning is managed
separately from the reusable workflow.

### Codex runner

Build and run the runner separately from the orchestrator — see
[`runner/README.md`](../runner/README.md) for per-container configuration
(model selection, MCP server registration). Its Codex authentication is
stored in a named volume, shared across every ephemeral spawn so login only
needs to happen once.

```bash
docker build -t variflex-runner:latest runner/
docker volume create variflex-runner-auth

docker run --rm -it \
  -v variflex-runner-auth:/home/codex/.codex \
  variflex-runner:latest codex login --device-auth

docker run -d \
  --name variflex-runner \
  --restart unless-stopped \
  --privileged \
  --group-add "$(stat -c '%g' /var/run/docker.sock)" \
  -p 7000:7000 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v variflex-runner-auth:/home/codex/.codex \
  -e 'GITHUB_TOKEN=<redacted>' \
  variflex-runner:latest

docker exec variflex-runner docker version
docker exec variflex-runner docker ps
curl http://localhost:7000/codex/version
```

Privileged mode allows Codex's own `workspace-write` sandbox to create and
configure its nested Linux namespace. The host Docker socket and its group ID
give the non-root `codex` user access to the host daemon; the image contains
the Docker CLI and Buildx plugin, but no Docker daemon. Supply `GITHUB_TOKEN`
at runtime so the dispatched agent can clone, push, and open its PR. Test the
runner image with `docker build -t variflex-runner:test -f runner/Dockerfile.test runner/ && docker run --rm variflex-runner:test`.

In normal operation this manual flow is only needed once, for the initial
`codex login`. Actual task dispatch uses ephemeral, Dockhand-spawned runner
containers sharing this same `variflex-runner-auth` volume — see "Repository
queues and ephemeral runners" above.
