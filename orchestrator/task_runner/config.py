import json
import os
from dataclasses import dataclass
from typing import Any


def _non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-empty string")
    return value.strip()


@dataclass(frozen=True)
class DeployTarget:
    container: str
    volume: str
    port: int
    health_path: str | None = None
    expected_content: str | None = None
    human_promoted_only: bool = False

    @classmethod
    def from_dict(cls, value: dict[str, Any], target_name: str) -> "DeployTarget":
        if not isinstance(value, dict):
            raise ValueError(f"repo {target_name} target must be an object")
        container = _non_empty_string(value.get("container"), f"repo {target_name} container")
        volume = _non_empty_string(value.get("volume"), f"repo {target_name} volume")
        port = value.get("port")
        if not isinstance(port, int) or isinstance(port, bool) or port < 1:
            raise ValueError(f"repo {target_name} port must be a positive integer")
        health_path = value.get("health_path")
        if health_path is not None:
            health_path = _non_empty_string(health_path, f"repo {target_name} health_path")
            if not health_path.startswith("/"):
                raise ValueError(f"repo {target_name} health_path must start with /")
        expected_content = value.get("expected_content")
        if expected_content is not None:
            expected_content = _non_empty_string(
                expected_content, f"repo {target_name} expected_content"
            )
        human_promoted_only = value.get("human_promoted_only", False)
        if not isinstance(human_promoted_only, bool):
            raise ValueError(f"repo {target_name} human_promoted_only must be a boolean")
        return cls(container, volume, port, health_path, expected_content, human_promoted_only)


@dataclass(frozen=True)
class RepoConfig:
    repo: str
    runner: str
    dev: DeployTarget
    main: DeployTarget
    model: str | None = None
    mcp_servers: tuple[tuple[str, str], ...] = ()
    host: str = "github"
    host_base_url: str | None = None

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "RepoConfig":
        repo = _non_empty_string(value.get("repo"), "repo config repo")
        parts = repo.split("/")
        if len(parts) != 2 or not all(part.strip() == part and part for part in parts):
            raise ValueError("repo config repo must be owner/name")
        runner = _non_empty_string(value.get("runner"), "repo config runner")
        dev = DeployTarget.from_dict(value.get("dev"), "dev")
        main = DeployTarget.from_dict(value.get("main"), "main")
        model = value.get("model")
        if model is not None:
            model = _non_empty_string(model, "repo config model")
        raw_mcp_servers = value.get("mcp_servers", {})
        if not isinstance(raw_mcp_servers, dict) or not all(
            isinstance(name, str) and name.strip() and isinstance(url, str) and url.strip()
            for name, url in raw_mcp_servers.items()
        ):
            raise ValueError("repo config mcp_servers must be an object of name to URL")
        host = value.get("host", "github")
        if host not in {"github", "forgejo"}:
            raise ValueError("repo config host must be github or forgejo")
        host_base_url = value.get("host_base_url")
        if host == "forgejo":
            host_base_url = _non_empty_string(host_base_url, "repo config host_base_url")
        elif host_base_url is not None:
            raise ValueError("repo config host_base_url is only valid for forgejo")
        return cls(
            repo,
            runner,
            dev,
            main,
            model,
            tuple(raw_mcp_servers.items()),
            host,
            host_base_url,
        )


def parse_repo_configs(values: Any) -> list[RepoConfig]:
    if not isinstance(values, list) or not all(isinstance(value, dict) for value in values):
        raise ValueError("TASK_RUNNER_REPOS must be a JSON array of repo config objects")
    repos = [RepoConfig.from_dict(value) for value in values]
    repo_names = [repo.repo for repo in repos]
    if len(repo_names) != len(set(repo_names)):
        raise ValueError("TASK_RUNNER_REPOS must not contain duplicate repos")
    return repos


def parse_interval_seconds(value: Any) -> float:
    if isinstance(value, int | float) and not isinstance(value, bool):
        interval = float(value)
    elif isinstance(value, str):
        text = value.strip().lower()
        if not text:
            raise ValueError("scheduled task interval must not be empty")
        unit = text[-1]
        multiplier = {"s": 1, "m": 60, "h": 3600, "d": 86400}.get(unit)
        if multiplier is None:
            interval = float(text)
        else:
            interval = float(text[:-1]) * multiplier
    else:
        raise ValueError("scheduled task interval must be a number or string")
    if interval <= 0:
        raise ValueError("scheduled task interval must be greater than zero")
    return interval


@dataclass(frozen=True)
class ScheduledTask:
    name: str
    repo: str
    issue_number: int
    runner: str
    interval_seconds: float

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "ScheduledTask":
        name = value.get("name")
        repo = value.get("repo")
        issue_number = value.get("issue_number")
        runner = value.get("runner")
        interval = value.get("interval", value.get("interval_seconds"))
        if not isinstance(name, str) or not name:
            raise ValueError("scheduled task name must be a non-empty string")
        if not isinstance(repo, str) or "/" not in repo:
            raise ValueError("scheduled task repo must be owner/name")
        if not isinstance(issue_number, int) or issue_number < 1:
            raise ValueError("scheduled task issue_number must be a positive integer")
        if not isinstance(runner, str) or not runner:
            raise ValueError("scheduled task runner must be a non-empty string")
        if interval is None:
            raise ValueError("scheduled task interval is required")
        return cls(name, repo, issue_number, runner, parse_interval_seconds(interval))


@dataclass(frozen=True)
class OpsImageCheck:
    name: str
    runner: str
    repo: str
    issue_number: int
    registry: str
    repository: str
    stop_container: str
    start_container: str
    auth_volume: str
    buildkit_addr: str
    source_sha: str
    keep_tags: int
    insecure_tls: bool
    interval_seconds: float

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "OpsImageCheck":
        name = value.get("name")
        runner = value.get("runner")
        repo = value.get("repo")
        issue_number = value.get("issue_number")
        registry = value.get("registry")
        repository = value.get("repository", "variflex-runner")
        stop_container = value.get("stop_container", "variflex-runner")
        start_container = value.get("start_container", "variflex-runner")
        auth_volume = value.get("auth_volume", "pacific-shift-codex-runner-auth")
        buildkit_addr = value.get("buildkit_addr", "unix:///run/buildkit/buildkitd.sock")
        source_sha = value.get("source_sha") or os.getenv("TASK_RUNNER_SOURCE_SHA", "unknown")
        keep_tags = value.get("keep_tags", 2)
        insecure_tls = value.get("insecure_tls", False)
        interval = value.get("interval", value.get("interval_seconds"))
        if not isinstance(name, str) or not name:
            raise ValueError("ops image check name must be a non-empty string")
        if not isinstance(runner, str) or not runner:
            raise ValueError("ops image check runner must be a non-empty string")
        if not isinstance(repo, str) or "/" not in repo:
            raise ValueError("ops image check repo must be owner/name")
        if not isinstance(issue_number, int) or issue_number < 1:
            raise ValueError("ops image check issue_number must be a positive integer")
        if not isinstance(registry, str) or not registry:
            raise ValueError("ops image check registry must be a non-empty string")
        if not isinstance(repository, str) or not repository:
            raise ValueError("ops image check repository must be a non-empty string")
        if not isinstance(stop_container, str) or not stop_container:
            raise ValueError("ops image check stop_container must be a non-empty string")
        if not isinstance(start_container, str) or not start_container:
            raise ValueError("ops image check start_container must be a non-empty string")
        if not isinstance(auth_volume, str) or not auth_volume:
            raise ValueError("ops image check auth_volume must be a non-empty string")
        if not isinstance(buildkit_addr, str) or not buildkit_addr:
            raise ValueError("ops image check buildkit_addr must be a non-empty string")
        if not isinstance(source_sha, str) or not source_sha:
            raise ValueError("ops image check source_sha must be a non-empty string")
        if not isinstance(keep_tags, int) or keep_tags < 1:
            raise ValueError("ops image check keep_tags must be a positive integer")
        if not isinstance(insecure_tls, bool):
            raise ValueError("ops image check insecure_tls must be a boolean")
        if interval is None:
            raise ValueError("ops image check interval is required")
        return cls(
            name,
            runner,
            repo,
            issue_number,
            registry,
            repository,
            stop_container,
            start_container,
            auth_volume,
            buildkit_addr,
            source_sha,
            keep_tags,
            insecure_tls,
            parse_interval_seconds(interval),
        )


@dataclass(frozen=True)
class Settings:
    database_path: str = "/data/tasks.db"
    runners: dict[str, str] = None  # type: ignore[assignment]
    scheduled_tasks: list[ScheduledTask] = None  # type: ignore[assignment]
    ops_image_checks: list[OpsImageCheck] = None  # type: ignore[assignment]
    repos: list[RepoConfig] = None  # type: ignore[assignment]
    timeout_seconds: float = 600
    output_cap_bytes: int = 1_000_000
    poll_interval_seconds: float = 2
    github_token: str | None = None
    forgejo_token: str | None = None
    dockhand_url: str | None = None
    dockhand_token: str | None = None
    dockhand_env: int | None = None
    dockhand_verify_timeout_seconds: float = 60
    dockhand_verify_interval_seconds: float = 2
    max_concurrent_containers: int = 3
    runner_image: str = "variflex-runner:latest"
    runner_auth_volume: str = "pacific-shift-codex-runner-auth"
    runner_port: int = 7000
    runner_network: str = "bridge"

    def __post_init__(self) -> None:
        object.__setattr__(self, "runners", self.runners or {})
        object.__setattr__(self, "scheduled_tasks", self.scheduled_tasks or [])
        object.__setattr__(self, "ops_image_checks", self.ops_image_checks or [])
        object.__setattr__(self, "repos", self.repos or [])
        repo_names = [repo.repo for repo in self.repos]
        if len(repo_names) != len(set(repo_names)):
            raise ValueError("TASK_RUNNER_REPOS must not contain duplicate repos")
        if self.max_concurrent_containers < 1:
            raise ValueError("TASK_RUNNER_MAX_CONCURRENT_CONTAINERS must be a positive integer")
        if not self.runner_image.strip():
            raise ValueError("TASK_RUNNER_RUNNER_IMAGE must be a non-empty string")
        if not self.runner_auth_volume.strip():
            raise ValueError("TASK_RUNNER_RUNNER_AUTH_VOLUME must be a non-empty string")
        if self.runner_port < 1:
            raise ValueError("TASK_RUNNER_RUNNER_PORT must be a positive integer")
        if not self.runner_network.strip():
            raise ValueError("TASK_RUNNER_RUNNER_NETWORK must be a non-empty string")

    @classmethod
    def from_env(cls) -> "Settings":
        raw_runners = os.getenv("TASK_RUNNER_RUNNERS", "{}")
        runners = json.loads(raw_runners)
        if not isinstance(runners, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in runners.items()
        ):
            raise ValueError("TASK_RUNNER_RUNNERS must be a JSON object of name to URL")
        raw_scheduled_tasks = os.getenv("TASK_RUNNER_SCHEDULED_TASKS", "[]")
        scheduled_task_values = json.loads(raw_scheduled_tasks)
        if not isinstance(scheduled_task_values, list) or not all(
            isinstance(value, dict) for value in scheduled_task_values
        ):
            raise ValueError("TASK_RUNNER_SCHEDULED_TASKS must be a JSON array of scheduled task objects")
        raw_ops_image_checks = os.getenv("TASK_RUNNER_OPS_IMAGE_CHECKS", "[]")
        ops_image_check_values = json.loads(raw_ops_image_checks)
        if not isinstance(ops_image_check_values, list) or not all(
            isinstance(value, dict) for value in ops_image_check_values
        ):
            raise ValueError("TASK_RUNNER_OPS_IMAGE_CHECKS must be a JSON array of ops image check objects")
        raw_repos = os.getenv("TASK_RUNNER_REPOS", "[]")
        repo_values = json.loads(raw_repos)
        repos = parse_repo_configs(repo_values)
        raw_dockhand_env = os.getenv("TASK_RUNNER_DOCKHAND_ENV")
        dockhand_env = int(raw_dockhand_env) if raw_dockhand_env else None
        return cls(
            database_path=os.getenv("TASK_RUNNER_DATABASE", "/data/tasks.db"),
            runners=runners,
            scheduled_tasks=[ScheduledTask.from_dict(value) for value in scheduled_task_values],
            ops_image_checks=[OpsImageCheck.from_dict(value) for value in ops_image_check_values],
            repos=repos,
            timeout_seconds=float(os.getenv("TASK_RUNNER_TIMEOUT_SECONDS", "600")),
            output_cap_bytes=int(os.getenv("TASK_RUNNER_OUTPUT_CAP_BYTES", "1000000")),
            poll_interval_seconds=float(os.getenv("TASK_RUNNER_POLL_INTERVAL_SECONDS", "2")),
            github_token=os.getenv("GITHUB_TOKEN"),
            forgejo_token=os.getenv("FORGEJO_TOKEN"),
            dockhand_url=os.getenv("TASK_RUNNER_DOCKHAND_URL"),
            dockhand_token=os.getenv("TASK_RUNNER_DOCKHAND_TOKEN"),
            dockhand_env=dockhand_env,
            dockhand_verify_timeout_seconds=float(
                os.getenv("TASK_RUNNER_DOCKHAND_VERIFY_TIMEOUT_SECONDS", "60")
            ),
            dockhand_verify_interval_seconds=float(
                os.getenv("TASK_RUNNER_DOCKHAND_VERIFY_INTERVAL_SECONDS", "2")
            ),
            max_concurrent_containers=int(
                os.getenv("TASK_RUNNER_MAX_CONCURRENT_CONTAINERS", "3")
            ),
            runner_image=os.getenv("TASK_RUNNER_RUNNER_IMAGE", "variflex-runner:latest"),
            runner_auth_volume=os.getenv(
                "TASK_RUNNER_RUNNER_AUTH_VOLUME", "pacific-shift-codex-runner-auth"
            ),
            runner_port=int(os.getenv("TASK_RUNNER_RUNNER_PORT", "7000")),
            runner_network=os.getenv("TASK_RUNNER_RUNNER_NETWORK", "bridge"),
        )
