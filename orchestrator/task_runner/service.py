import asyncio
import base64
import json
import logging
import os
import tempfile
import uuid
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

from .config import OpsImageCheck, RepoConfig, ScheduledTask, Settings, parse_repo_configs
from .database import Database
from .dockhand import ContainerDeployResult, ContainerSnapshot, DockhandClient
from .git_host import GitHostClient
from .ops_images import codex_runner_tag
from .runner import RunnerClient


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RunnerQueue:
    pending: deque[str] = field(default_factory=deque)
    active_task_id: str | None = None
    halt_state: Literal["halted", "quota_halted"] | None = None
    halt_reason: str | None = None
    resumes_at: str | None = None


@dataclass(frozen=True)
class OpsImageRebuildJob:
    check: OpsImageCheck
    installed_version: str
    target_version: str


class TaskService:
    def __init__(
        self,
        settings: Settings,
        database: Database,
        git_hosts: dict[str, GitHostClient],
        runner: RunnerClient,
        dockhand: DockhandClient | None = None,
    ):
        self.settings = settings
        self.database = database
        self.git_hosts = git_hosts
        self.runner = runner
        self.dockhand = dockhand
        self._jobs: set[asyncio.Task] = set()
        self._scheduler_jobs: set[asyncio.Task] = set()
        self._internal_ops_jobs: dict[str, OpsImageRebuildJob] = {}
        self._queue_lock = asyncio.Lock()
        self._repo_queues = {repo.repo: RunnerQueue() for repo in settings.repos}
        self._repos = list(settings.repos)
        self.logger = logging.getLogger(__name__)

    def initialize_repo_registry(self) -> None:
        seed = [self._repo_to_dict(repo) for repo in self.settings.repos]
        self.database.seed_repo_configs(seed)
        self._repos = parse_repo_configs(self.database.load_repo_configs())
        self._validate_repo_set(self._repos)
        for repo in self._repos:
            self._repo_queues.setdefault(repo.repo, RunnerQueue())

    def start_scheduler(self) -> None:
        if self._scheduler_jobs:
            return
        for scheduled_task in self.settings.scheduled_tasks:
            job = asyncio.create_task(self._scheduled_loop(scheduled_task))
            self._scheduler_jobs.add(job)
            job.add_done_callback(self._scheduler_jobs.discard)
            self.logger.info(
                "Scheduled task '%s' enabled for %s#%s on runner '%s' every %gs",
                scheduled_task.name,
                scheduled_task.repo,
                scheduled_task.issue_number,
                scheduled_task.runner,
                scheduled_task.interval_seconds,
            )
        for ops_check in self.settings.ops_image_checks:
            job = asyncio.create_task(self._ops_image_loop(ops_check))
            self._scheduler_jobs.add(job)
            job.add_done_callback(self._scheduler_jobs.discard)
            self.logger.info(
                "Ops image check '%s' enabled for runner '%s' every %gs",
                ops_check.name,
                ops_check.runner,
                ops_check.interval_seconds,
            )

    def resume_running_tasks(self) -> None:
        persisted = self.database.load_runner_queues()
        # Current rows are keyed by repo. Legacy rows were keyed by runner, so
        # distribute their task IDs by the task's persisted repo during migration.
        for key, state in persisted.items():
            task_ids = ([state["active_task_id"]] if state["active_task_id"] else []) + state["pending"]
            if not task_ids and key in self._repo_queues:
                queue = self._repo_queues[key]
                queue.halt_state = state["halt_state"]
                queue.halt_reason = state["halt_reason"]
                queue.resumes_at = state["resumes_at"]
                continue
            grouped: dict[str, list[str]] = {}
            for task_id in task_ids:
                task = self.database.get(task_id)
                if task:
                    grouped.setdefault(task["repo"], []).append(task_id)
            for repo, ids in grouped.items():
                queue = self._repo_queues.setdefault(repo, RunnerQueue())
                active = state["active_task_id"] if state["active_task_id"] in ids else None
                queue.active_task_id = active
                queue.pending.extend(task_id for task_id in ids if task_id != active)
                queue.halt_state = state["halt_state"]
                queue.halt_reason = state["halt_reason"]
                queue.resumes_at = state["resumes_at"]
                self._persist_queue(repo, queue)
            if grouped and key not in grouped:
                self.database.delete_runner_queue(key)
        represented = {
            task_id
            for queue in self._repo_queues.values()
            for task_id in ([queue.active_task_id] if queue.active_task_id else [])
        }
        for task in self.database.list():
            if task["status"] == "running" and task["id"] not in represented:
                queue = self._repo_queues.setdefault(task["repo"], RunnerQueue())
                if queue.active_task_id is None:
                    queue.active_task_id = task["id"]
                    self._persist_queue(task["repo"], queue)
        for repo, queue in self._repo_queues.items():
            if queue.active_task_id:
                job = asyncio.create_task(self._process_repo_task(repo, queue.active_task_id))
                self._register_queue_job(job, repo)
            elif queue.halt_state == "quota_halted" and queue.resumes_at:
                resume_time = self._parse_resume_time(queue.resumes_at)
                if resume_time:
                    job = asyncio.create_task(self._resume_quota_halted_queue(repo, resume_time))
                    self._jobs.add(job)
                    job.add_done_callback(self._jobs.discard)
        schedule_job = asyncio.create_task(self._schedule_eligible())
        self._jobs.add(schedule_job)
        schedule_job.add_done_callback(self._jobs.discard)

    def _register_queue_job(self, job: asyncio.Task, repo: str) -> None:
        setattr(job, "_task_runner_repo", repo)
        self._jobs.add(job)
        job.add_done_callback(self._jobs.discard)

    def _persist_queue(self, repo: str, queue: RunnerQueue) -> None:
        self.database.save_runner_queue(
            repo, list(queue.pending), queue.active_task_id,
            queue.halt_state, queue.halt_reason, queue.resumes_at,
        )

    async def stop_scheduler(self) -> None:
        if not self._scheduler_jobs:
            return
        for job in self._scheduler_jobs:
            job.cancel()
        await asyncio.gather(*self._scheduler_jobs, return_exceptions=True)
        self._scheduler_jobs.clear()

    async def _scheduled_loop(self, scheduled_task: ScheduledTask) -> None:
        while True:
            await asyncio.sleep(scheduled_task.interval_seconds)
            try:
                receipt = await self.run_task(
                    scheduled_task.repo, scheduled_task.issue_number, scheduled_task.runner
                )
                self.logger.info(
                    "Scheduled task '%s' fired and created task %s",
                    scheduled_task.name,
                    receipt["task_id"],
                )
            except Exception:
                self.logger.exception("Scheduled task '%s' failed to fire", scheduled_task.name)

    async def _ops_image_loop(self, ops_check: OpsImageCheck) -> None:
        while True:
            await asyncio.sleep(ops_check.interval_seconds)
            try:
                dispatched = await self.check_ops_image(ops_check)
                if dispatched:
                    self.logger.info("Ops image check '%s' detected drift and dispatched rebuild", ops_check.name)
                else:
                    self.logger.info("Ops image check '%s' completed with no drift", ops_check.name)
            except Exception:
                self.logger.exception("Ops image check '%s' failed", ops_check.name)

    async def run_task(self, repo: str, issue_number: int, runner_name: str) -> dict[str, Any]:
        if runner_name not in self.settings.runners:
            available = ", ".join(sorted(self.settings.runners)) or "none"
            raise ValueError(f"Unknown runner '{runner_name}'. Available runners: {available}")
        repo_config = self.get_repo_config(repo)
        if runner_name != repo_config.runner:
            raise ValueError(
                f"Repo '{repo}' is configured for runner '{repo_config.runner}', not '{runner_name}'"
            )
        if "/" not in repo or issue_number < 1:
            raise ValueError("repo must be owner/name and issue_number must be positive")
        task_id = str(uuid.uuid4())
        self.database.create_task(task_id, repo, issue_number, runner_name, "")
        async with self._queue_lock:
            queue = self._repo_queues.setdefault(repo, RunnerQueue())
            queue.pending.append(task_id)
            self._persist_queue(repo, queue)
            queue_length = len(queue.pending) + (1 if queue.active_task_id else 0)
            position = queue_length - 1
            status = "running" if self._can_admit(repo) and queue.pending[0] == task_id else "queued"
            resumes_at = queue.resumes_at if queue.halt_state == "quota_halted" else None
        await self._schedule_eligible()
        receipt = {
            "task_id": task_id,
            "status": status,
            "position": position,
            "queue_length": queue_length,
            "runner": runner_name,
        }
        if resumes_at is not None:
            receipt["resumes_at"] = resumes_at
        return receipt

    def get_repo_config(self, repo: str) -> RepoConfig:
        for repo_config in self._repos:
            if repo_config.repo == repo:
                return repo_config
        available = ", ".join(sorted(item.repo for item in self._repos)) or "none"
        raise ValueError(
            f"Repo '{repo}' is not registered in TASK_RUNNER_REPOS. Registered repos: {available}"
        )

    def list_repo_configs(self) -> list[dict[str, Any]]:
        return [self._repo_to_dict(repo) for repo in self._repos]

    @staticmethod
    def _repo_to_dict(repo: RepoConfig) -> dict[str, Any]:
        value = asdict(repo)
        value["mcp_servers"] = dict(repo.mcp_servers)
        return value

    def replace_repo_configs(self, values: Any) -> list[dict[str, Any]]:
        repos = parse_repo_configs(values)
        self._validate_repo_set(repos)
        serialized = [self._repo_to_dict(repo) for repo in repos]
        self.database.replace_repo_configs(serialized)
        self._repos = repos
        for repo in repos:
            self._repo_queues.setdefault(repo.repo, RunnerQueue())
        return serialized

    @staticmethod
    def _validate_repo_set(repos: list[RepoConfig]) -> None:
        forgejo_base_urls = {
            repo.host_base_url for repo in repos if repo.host == "forgejo"
        }
        if len(forgejo_base_urls) > 1:
            raise ValueError("Configured Forgejo repositories must use the same host_base_url")

    def _can_admit(self, repo: str) -> bool:
        queue = self._repo_queues[repo]
        active = sum(bool(item.active_task_id) for item in self._repo_queues.values())
        return queue.halt_state is None and queue.active_task_id is None and active < self.settings.max_concurrent_containers

    async def clear_runner_halt(self, repo: str) -> dict[str, Any]:
        if repo not in self._repo_queues:
            available = ", ".join(sorted(self._repo_queues)) or "none"
            raise ValueError(f"Unknown repo '{repo}'. Available repos: {available}")
        async with self._queue_lock:
            queue = self._repo_queues.setdefault(repo, RunnerQueue())
            previous_halt_state = queue.halt_state
            if previous_halt_state is None:
                return {
                    "repo": repo,
                    "status": "not_halted",
                    "pending_count": len(queue.pending),
                }
            queue.halt_state = None
            queue.halt_reason = None
            queue.resumes_at = None
            self._persist_queue(repo, queue)
            pending_count = len(queue.pending)
        await self._schedule_eligible()
        self.logger.info("Repo queue '%s' halt cleared manually", repo)
        return {
            "repo": repo,
            "status": "resumed",
            "previous_halt_state": previous_halt_state,
            "pending_count": pending_count,
        }

    async def cancel_queued_task(self, task_id: str) -> dict[str, Any]:
        task = self._required(task_id)
        repo = task["repo"]
        async with self._queue_lock:
            queue = self._repo_queues.setdefault(repo, RunnerQueue())
            if queue.active_task_id == task_id:
                raise ValueError(
                    f"Task '{task_id}' is active and cannot be cancelled as a queued task"
                )
            try:
                queue.pending.remove(task_id)
            except ValueError:
                raise ValueError(f"Task '{task_id}' is not pending in a repo queue") from None
            pending_count = len(queue.pending)
            self._internal_ops_jobs.pop(task_id, None)
            self.database.update(
                task_id,
                status="cancelled",
                error="Cancelled while queued before runner execution.",
                completed_at=utcnow(),
            )
            self._persist_queue(repo, queue)
        self.logger.info("Cancelled queued task %s for repo '%s'", task_id, repo)
        return {
            "task_id": task_id,
            "repo": repo,
            "status": "cancelled",
            "pending_count": pending_count,
        }

    async def _schedule_eligible(self) -> None:
        async with self._queue_lock:
            while sum(bool(q.active_task_id) for q in self._repo_queues.values()) < self.settings.max_concurrent_containers:
                candidates = [
                    ((self.database.get(q.pending[0]) or {}).get("created_at", ""), repo)
                    for repo, q in self._repo_queues.items()
                    if q.pending and q.active_task_id is None and q.halt_state is None
                ]
                if not candidates:
                    return
                _, repo = min(candidates)
                queue = self._repo_queues[repo]
                task_id = queue.pending.popleft()
                queue.active_task_id = task_id
                self._persist_queue(repo, queue)
                job = asyncio.create_task(self._process_repo_task(repo, task_id))
                self._register_queue_job(job, repo)

    async def _process_repo_task(self, repo: str, task_id: str) -> None:
        try:
            await self._dispatch_or_run_internal(task_id)
            task = self.database.get(task_id) or {}
            if task.get("status") != "completed":
                resets_at = task.get("resets_at")
                resume_time = self._parse_resume_time(resets_at) if resets_at else None
                if (
                    task.get("status") == "quota_exceeded"
                    and task.get("quota_auto_resume")
                    and task.get("session_id")
                    and resume_time is not None
                ):
                    self.logger.warning(
                        "Repo queue '%s' quota-halted after task %s until %s",
                        repo,
                        task_id,
                        resets_at,
                    )
                    async with self._queue_lock:
                        queue = self._repo_queues.setdefault(repo, RunnerQueue())
                        queue.pending.appendleft(task_id)
                        queue.halt_state = "quota_halted"
                        queue.halt_reason = task.get("error") or "Runner quota exceeded."
                        queue.resumes_at = str(resets_at)
                        self._persist_queue(repo, queue)
                    resume_job = asyncio.create_task(
                        self._resume_quota_halted_queue(repo, resume_time)
                    )
                    self._jobs.add(resume_job)
                    resume_job.add_done_callback(self._jobs.discard)
                    return
                self.logger.error(
                    "Repo queue '%s' halted after task %s ended with status '%s': %s",
                    repo,
                    task_id,
                    task.get("status"),
                    task.get("error") or "no error detail recorded",
                )
                async with self._queue_lock:
                    queue = self._repo_queues.setdefault(repo, RunnerQueue())
                    queue.halt_state = "halted"
                    queue.halt_reason = (
                        task.get("error") or f"Task ended with status {task.get('status')}"
                    )
                    self._persist_queue(repo, queue)
                return
        except Exception:
            self.logger.exception("Repo queue '%s' halted while processing task %s", repo, task_id)
            async with self._queue_lock:
                queue = self._repo_queues.setdefault(repo, RunnerQueue())
                queue.halt_state = "halted"
                queue.halt_reason = f"Exception while processing task {task_id}"
                self._persist_queue(repo, queue)
            return
        finally:
            async with self._queue_lock:
                queue = self._repo_queues.setdefault(repo, RunnerQueue())
                if queue.active_task_id == task_id:
                    queue.active_task_id = None
                    self._persist_queue(repo, queue)
            await self._schedule_eligible()

    @staticmethod
    def _parse_resume_time(value: str) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return None
        return parsed

    async def _resume_quota_halted_queue(self, repo: str, resume_time: datetime) -> None:
        delay = max(0.0, (resume_time.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds())
        await asyncio.sleep(delay)
        while True:
            async with self._queue_lock:
                queue = self._repo_queues.setdefault(repo, RunnerQueue())
                if queue.halt_state != "quota_halted":
                    return
                if queue.active_task_id is None:
                    queue.halt_state = None
                    queue.halt_reason = None
                    queue.resumes_at = None
                    self._persist_queue(repo, queue)
                    break
            await asyncio.sleep(0)
        self.logger.info("Repo queue '%s' resumed after quota reset", repo)
        await self._schedule_eligible()

    async def _dispatch_or_run_internal(self, task_id: str) -> None:
        ops_job = self._internal_ops_jobs.get(task_id)
        if ops_job is not None:
            await self._run_ops_image_rebuild(task_id, ops_job)
            return
        await self._dispatch(task_id)

    async def _dispatch(self, task_id: str) -> None:
        task = self.database.get(task_id)
        assert task is not None
        managed_container = bool(task.get("runner_container"))
        container_name = task.get("runner_container") or f"task-runner-{task_id[:12]}"
        runner_url: str | None = task.get("runner_url") or None
        try:
            if task["status"] == "running" and runner_url and task.get("execution_id"):
                await self._resume_monitor(task_id, runner_url, task["execution_id"])
                return
            if self.dockhand is None:
                raise RuntimeError("Dockhand runner lifecycle capability is not configured")
            repo_config = self.get_repo_config(task["repo"])
            environment = {
                "TASK_RUNNER_TASK_ID": task_id,
                "TASK_RUNNER_TARGET_REPO": task["repo"],
                "TASK_RUNNER_GIT_HOST": repo_config.host,
            }
            if repo_config.host_base_url:
                environment["TASK_RUNNER_GIT_HOST_BASE_URL"] = repo_config.host_base_url
            if repo_config.model:
                environment["CODEX_RUNNER_MODEL"] = repo_config.model
            if repo_config.mcp_servers:
                environment["CODEX_RUNNER_MCP_SERVERS"] = json.dumps(dict(repo_config.mcp_servers))
            if self.settings.github_token:
                environment["GITHUB_TOKEN"] = self.settings.github_token
            if self.settings.forgejo_token:
                environment["FORGEJO_TOKEN"] = self.settings.forgejo_token
            self.database.update(task_id, status="spawning", runner_container=container_name)
            managed_container = True
            runner_url = await self.dockhand.spawn_runner(
                name=container_name,
                image=self.settings.runner_image,
                auth_volume=self.settings.runner_auth_volume,
                network=self.settings.runner_network,
                environment=environment,
                port=self.settings.runner_port,
            )
            wait_until_ready = getattr(self.runner, "wait_until_ready", None)
            if wait_until_ready is not None:
                await wait_until_ready(
                    runner_url,
                    self.settings.dockhand_verify_timeout_seconds,
                    self.settings.dockhand_verify_interval_seconds,
                )
            self.database.update(task_id, runner_url=runner_url)
            if task["status"] == "quota_exceeded" and task.get("session_id"):
                prompt = task.get("prompt") or ""
                self.logger.info(
                    "Resuming quota-interrupted task %s with Codex session %s",
                    task_id,
                    task["session_id"],
                )
                self.database.update(task_id, status="dispatching", completed_at=None)
                execution_id = await self.runner.resume(
                    runner_url, task["repo"], task["issue_number"], prompt, task["session_id"]
                )
            else:
                repo_config = self.get_repo_config(task["repo"])
                git_host = self.git_hosts[repo_config.host]
                issue = await git_host.get_issue_context(task["repo"], task["issue_number"])
                agents = await git_host.get_text_file(task["repo"], "AGENTS.md")
                agents = agents or "(No AGENTS.md found in the target repository.)"
                title, body = issue.title, issue.body
                prompt = self.build_prompt(
                    task["repo"],
                    task["issue_number"],
                    agents,
                    title,
                    body,
                    repo_config.host,
                    repo_config.host_base_url,
                )
                self.database.update(task_id, status="dispatching", prompt=prompt, started_at=utcnow())
                execution_id = await self.runner.execute(
                    runner_url, task["repo"], task["issue_number"], prompt
                )
            self.database.update(task_id, status="running", execution_id=execution_id)
            await self._monitor_with_timeout(task_id, runner_url, execution_id, self.settings.timeout_seconds)
        except asyncio.TimeoutError:
            if runner_url:
                await self._record_timeout(task_id, runner_url)
            else:
                self.database.update(task_id, status="timeout", error="Runner spawn timed out.", completed_at=utcnow())
        except Exception as exc:
            self.database.update(task_id, status="failed", error=f"{type(exc).__name__}: {exc}", completed_at=utcnow())
        finally:
            if self.dockhand is not None and managed_container:
                try:
                    await self.dockhand.destroy_runner(container_name)
                except Exception as exc:
                    current = self.database.get(task_id) or {}
                    cleanup_error = f"Runner cleanup failed: {type(exc).__name__}: {exc}"
                    prior = current.get("error")
                    self.database.update(
                        task_id,
                        status="failed",
                        error=f"{prior}; {cleanup_error}" if prior else cleanup_error,
                        completed_at=utcnow(),
                    )

    async def _run_ops_image_rebuild(self, task_id: str, job: OpsImageRebuildJob) -> None:
        check = job.check
        prompt = self.build_ops_rebuild_prompt(check, job.installed_version, job.target_version)
        log_parts: list[str] = []
        snapshot: ContainerSnapshot | None = None
        replacement_started = False
        self.database.update(task_id, status="running", prompt=prompt, started_at=utcnow())
        try:
            if self.dockhand is None:
                raise RuntimeError("Dockhand deploy capability is not configured")
            repo_sha = check.source_sha[:7]
            tag = codex_runner_tag(job.target_version, repo_sha)
            registry_host = _registry_host(check.registry)
            image = f"{registry_host}/{check.repository}:{tag}"
            log_parts.append(
                "\n".join(
                    [
                        "Ops Images codex-runner rebuild started.",
                        f"Trace issue: {check.repo}#{check.issue_number}",
                        f"Runner: {check.runner}",
                        f"Installed Codex version: {job.installed_version}",
                        f"Target Codex version: {job.target_version}",
                        f"Image: {image}",
                    ]
                )
            )
            if not await self.dockhand.container_uses_volume(check.start_container, check.auth_volume):
                raise RuntimeError(
                    f"Container '{check.start_container}' is not configured with required volume '{check.auth_volume}'"
                )
            log_parts.append(f"Verified required auth volume before deploy: {check.auth_volume}")
            if not self.settings.github_token:
                raise RuntimeError("GITHUB_TOKEN is required to clone the Variflex runner source")
            credentials = base64.b64encode(
                f"x-access-token:{self.settings.github_token}".encode()
            ).decode()
            git_env = {
                "GIT_CONFIG_COUNT": "1",
                "GIT_CONFIG_KEY_0": "http.https://github.com/.extraheader",
                "GIT_CONFIG_VALUE_0": f"AUTHORIZATION: basic {credentials}",
            }
            with tempfile.TemporaryDirectory(prefix="variflex-runner-build-") as workspace:
                repo_dir = os.path.join(workspace, "variflex")
                source_dir = os.path.join(repo_dir, "runner")
                clone_command = [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "--branch",
                    "main",
                    "--single-branch",
                    "https://github.com/m3rrym4n/variflex.git",
                    repo_dir,
                ]
                log_parts.append(await self._run_command(clone_command, env=git_env))
                build_command = [
                    "buildctl",
                    "--addr",
                    check.buildkit_addr,
                    "build",
                    "--frontend",
                    "dockerfile.v0",
                    "--local",
                    f"context={source_dir}",
                    "--local",
                    f"dockerfile={source_dir}",
                    "--opt",
                    f"build-arg:CODEX_VERSION={job.target_version}",
                    "--output",
                    f"type=image,name={image},push=true",
                ]
                log_parts.append(await self._run_command(build_command))
            prune_command = [
                "python",
                "/app/scripts/prune_zot_image_tags.py",
                "--registry",
                _registry_url(check.registry),
                "--repository",
                check.repository,
                "--keep",
                str(check.keep_tags),
            ]
            if check.insecure_tls:
                prune_command.append("--insecure-tls")
            log_parts.append(await self._run_command(prune_command))
            if check.stop_container != check.start_container:
                raise RuntimeError("Ops Images replacement requires stop_container and start_container to match")
            await self.dockhand.pull_image(image)
            log_parts.append(f"Pulled rebuilt image on deploy host: {image}")
            snapshot = await self.dockhand.snapshot_container(check.start_container)
            log_parts.append(f"Captured rollback snapshot: container={snapshot.name}, image={snapshot.image}")
            replacement_started = True
            deploy_result = await self.dockhand.replace_from_snapshot(snapshot, image)
            log_parts.append(
                "Deploy verified: "
                f"stopped={deploy_result.stopped_container}, started={deploy_result.started_container}, "
                f"status={deploy_result.status}, health={deploy_result.health_status}"
            )
            if not await self.dockhand.container_uses_volume(check.start_container, check.auth_volume):
                raise RuntimeError(
                    f"Required volume '{check.auth_volume}' is missing after deploy on '{check.start_container}'"
                )
            log_parts.append(f"Verified required auth volume after deploy: {check.auth_volume}")
            log = "\n\n".join(log_parts)
            capped_log, truncated = self._cap(log)
            self.database.update(
                task_id,
                status="completed",
                result=f"Built, pushed, pruned, and deployed {image}",
                log=capped_log,
                output_truncated=int(truncated),
                completed_at=utcnow(),
            )
        except Exception as exc:
            rollback_error: Exception | None = None
            if replacement_started and snapshot is not None and self.dockhand is not None:
                log_parts.append(f"Deployment failed: {type(exc).__name__}: {exc}")
                log_parts.append(f"Rollback started: restoring {snapshot.name} on {snapshot.image}")
                try:
                    restored = await self.dockhand.restore_snapshot(snapshot)
                    log_parts.append(
                        "Rollback verified from actual container state: "
                        f"container={snapshot.name}, running={restored.running}, "
                        f"status={restored.status}, health={restored.health_status}, image={snapshot.image}"
                    )
                except Exception as restore_exc:
                    rollback_error = restore_exc
                    log_parts.append(
                        "ALERT: rollback failed; manual recovery required: "
                        f"{type(restore_exc).__name__}: {restore_exc}"
                    )
            log = "\n\n".join(log_parts)
            capped_log, truncated = self._cap(log)
            self.database.update(
                task_id,
                status="failed",
                log=capped_log,
                output_truncated=int(truncated),
                error=(
                    f"{type(exc).__name__}: {exc}"
                    if rollback_error is None
                    else f"{type(exc).__name__}: {exc}; rollback failed: "
                    f"{type(rollback_error).__name__}: {rollback_error}"
                ),
                completed_at=utcnow(),
            )
        finally:
            self._internal_ops_jobs.pop(task_id, None)

    async def _run_command(self, command: list[str], env: dict[str, str] | None = None) -> str:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            env=None if env is None else {**os.environ, **env},
        )
        stdout, _ = await process.communicate()
        output = stdout.decode("utf-8", errors="replace")
        command_text = " ".join(command)
        if process.returncode != 0:
            raise RuntimeError(f"Command failed ({process.returncode}): {command_text}\n{output}")
        return f"$ {command_text}\n{output}".strip()

    async def _resume_monitor(self, task_id: str, url: str, execution_id: str) -> None:
        try:
            if await self._record_terminal_if_available(task_id, url, execution_id):
                return
            remaining_timeout = self._remaining_timeout(task_id)
            if remaining_timeout <= 0:
                await self._record_timeout(task_id, url)
                return
            await self._monitor_with_timeout(task_id, url, execution_id, remaining_timeout)
        except asyncio.TimeoutError:
            await self._record_timeout(task_id, url)
        except Exception as exc:
            self.database.update(task_id, status="failed", error=f"{type(exc).__name__}: {exc}", completed_at=utcnow())

    async def _monitor_with_timeout(self, task_id: str, url: str, execution_id: str, timeout: float) -> None:
        await asyncio.wait_for(self._monitor(task_id, url, execution_id), timeout=timeout)

    async def _monitor(self, task_id: str, url: str, execution_id: str) -> None:
        while True:
            if await self._record_terminal_if_available(task_id, url, execution_id):
                return
            await asyncio.sleep(self.settings.poll_interval_seconds)

    async def _record_terminal_if_available(self, task_id: str, url: str, execution_id: str) -> bool:
        status_data = await self.runner.status(url, execution_id)
        status = status_data.get("status")
        if status not in {"completed", "failed", "timeout", "quota_exceeded"}:
            return False
        result_data = await self.runner.result(url, execution_id)
        result = result_data.get("result") or result_data.get("report") or ""
        log = result_data.get("log") or result_data.get("stdout") or ""
        capped_log, truncated = self._cap(str(log))
        self.database.update(
            task_id, status=status, result=str(result), log=capped_log,
            output_truncated=int(truncated), error=result_data.get("error"),
            resets_at=result_data.get("resets_at"), completed_at=utcnow(),
            session_id=result_data.get("session_id"),
            branch=result_data.get("branch"), pr_url=result_data.get("pr_url"),
            quota_auto_resume=int(bool(result_data.get("quota_auto_resume"))),
        )
        return True

    async def _record_timeout(self, task_id: str, url: str) -> None:
        current = self.database.get(task_id) or {}
        execution_id = current.get("execution_id")
        cancelled = bool(execution_id) and await self.runner.cancel(url, execution_id)
        detail = "Runner cancellation accepted." if cancelled else "Runner cancellation unavailable or rejected."
        self.database.update(task_id, status="timeout", error=f"Task exceeded {self.settings.timeout_seconds:g}s. {detail}", completed_at=utcnow())

    def _remaining_timeout(self, task_id: str) -> float:
        task = self.database.get(task_id) or {}
        started_at = task.get("started_at")
        if not started_at:
            return self.settings.timeout_seconds
        started = datetime.fromisoformat(str(started_at))
        if started.tzinfo is None:
            started = started.replace(tzinfo=timezone.utc)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
        return max(0, self.settings.timeout_seconds - elapsed)

    def _cap(self, text: str) -> tuple[str, bool]:
        encoded = text.encode("utf-8")
        if len(encoded) <= self.settings.output_cap_bytes:
            return text, False
        marker = f"\n[OUTPUT TRUNCATED: exceeded {self.settings.output_cap_bytes} bytes]"
        budget = max(0, self.settings.output_cap_bytes - len(marker.encode()))
        shortened = encoded[:budget].decode("utf-8", errors="ignore")
        return shortened + marker, True

    @staticmethod
    def build_prompt(
        repo: str,
        issue_number: int,
        agents: str,
        title: str,
        body: str,
        host: str = "github",
        host_base_url: str | None = None,
    ) -> str:
        host_name = "GitHub" if host == "github" else "Forgejo"
        repo_location = (
            repo if host_base_url is None else f"{host_base_url.rstrip('/')}/{repo}"
        )
        return f"""# Task

Work on {host_name} issue #{issue_number} in {repo_location}.

## Repository instructions (AGENTS.md)

{agents}

## {host_name} issue #{issue_number}: {title}

{body}

Follow the repository instructions and issue acceptance criteria. Keep work within this issue's scope. Run the required tests and provide the required structured final report.
"""

    @staticmethod
    def build_ops_rebuild_prompt(check: OpsImageCheck, installed: str, target: str) -> str:
        return f"""# Internal Ops Images rebuild

Trace issue: {check.repo}#{check.issue_number}
Runner queue: {check.runner}
Installed Codex version: {installed}
Target Codex version: {target}

This is an internal maintenance job created by Variflex after version drift
was detected. It is tied to the configured trace issue so every rebuild cycle
has a durable written reference in the normal task log/result model.
"""

    def get_task_result(self, task_id: str) -> dict[str, Any]:
        task = self._required(task_id)
        return {
            key: task[key]
            for key in (
                "id", "status", "result", "error", "resets_at", "session_id", "output_truncated",
                "created_at", "completed_at",
            )
        }

    def get_task_log(self, task_id: str) -> dict[str, Any]:
        task = self._required(task_id)
        return {"id": task_id, "status": task["status"], "log": task["log"], "output_truncated": bool(task["output_truncated"])}

    def list_tasks(self) -> list[dict[str, Any]]:
        keys = ("id", "repo", "issue_number", "runner", "status", "output_truncated", "created_at", "completed_at")
        return [{key: task[key] for key in keys} for task in self.database.list()]

    def list_dashboard_tasks(
        self, window: Literal["24h", "7d", "30d", "all"], limit: int | None, offset: int
    ) -> dict[str, Any]:
        cutoffs = {"24h": 24, "7d": 24 * 7, "30d": 24 * 30}
        cutoff = None
        if window != "all":
            cutoff = datetime.now(timezone.utc).timestamp() - cutoffs[window] * 60 * 60

        matching = []
        for task in self.database.list():
            created_at = datetime.fromisoformat(str(task["created_at"]).replace("Z", "+00:00"))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            if cutoff is None or created_at.timestamp() >= cutoff:
                matching.append(task)

        running_count = sum(
            task["status"] in {"queued", "spawning", "dispatching", "running"}
            for task in matching
        )
        selected = matching[offset : offset + limit if limit is not None else None]
        keys = (
            "id", "repo", "issue_number", "runner", "status", "created_at", "completed_at",
            "branch", "pr_url", "resets_at", "session_id",
        )
        tasks = []
        for task in selected:
            item = {key: task[key] for key in keys}
            if item["status"] in {"queued", "spawning", "dispatching"}:
                item["status"] = "running"
            tasks.append(item)
        return {"tasks": tasks, "running_count": running_count}

    async def get_queue_states(self) -> dict[str, dict[str, Any]]:
        async with self._queue_lock:
            return {
                repo: {
                    "active_task_id": queue.active_task_id,
                    "pending": list(queue.pending),
                    "halt_state": queue.halt_state,
                    "resumes_at": queue.resumes_at,
                }
                for repo, queue in self._repo_queues.items()
            }

    async def deploy_container_swap(self, stop_container: str, start_container: str) -> ContainerDeployResult:
        if self.dockhand is None:
            raise RuntimeError("Dockhand deploy capability is not configured")
        return await self.dockhand.deploy_container_swap(stop_container, start_container)

    async def check_ops_image(self, ops_check: OpsImageCheck) -> bool:
        if ops_check.runner not in self.settings.runners:
            available = ", ".join(sorted(self.settings.runners)) or "none"
            raise ValueError(f"Unknown runner '{ops_check.runner}'. Available runners: {available}")
        version_data = await self.runner.codex_version(self.settings.runners[ops_check.runner])
        installed = str(version_data.get("installed") or "")
        latest = str(version_data.get("latest") or "")
        if not installed or not latest:
            raise ValueError("Codex version drift response must include installed and latest")
        drift_value = version_data.get("drift_detected")
        drift_detected = drift_value if isinstance(drift_value, bool) else installed != latest
        if not drift_detected:
            return False
        task_id = str(uuid.uuid4())
        self.database.create_task(
            task_id,
            ops_check.repo,
            ops_check.issue_number,
            ops_check.runner,
            self.settings.runners[ops_check.runner],
        )
        self._internal_ops_jobs[task_id] = OpsImageRebuildJob(ops_check, installed, latest)
        async with self._queue_lock:
            queue = self._repo_queues.setdefault(ops_check.repo, RunnerQueue())
            queue.pending.append(task_id)
            self._persist_queue(ops_check.repo, queue)
        await self._schedule_eligible()
        return True

    def _required(self, task_id: str) -> dict[str, Any]:
        task = self.database.get(task_id)
        if task is None:
            raise ValueError(f"Unknown task_id: {task_id}")
        return task


def _registry_host(registry: str) -> str:
    return registry.removeprefix("https://").removeprefix("http://").rstrip("/")


def _registry_url(registry: str) -> str:
    if registry.startswith("http://") or registry.startswith("https://"):
        return registry.rstrip("/")
    return f"https://{registry.rstrip('/')}"
