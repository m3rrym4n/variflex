import asyncio
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from task_runner.config import (
    DeployTarget,
    OpsImageCheck,
    RepoConfig,
    ScheduledTask,
    Settings,
    parse_interval_seconds,
)
from task_runner.database import Database
from task_runner.git_host import IssueContext
from task_runner.service import TaskService
from task_runner.dockhand import ContainerSnapshot, ContainerState


class FakeGitHub:
    def __init__(self, title="Test issue", body="acceptance criteria", agents="instructions"):
        self.workflow_dispatches = []
        self.context_requests = []
        self.file_requests = []
        self.title = title
        self.body = body
        self.agents = agents

    async def get_issue_context(self, repo, issue_number):
        self.context_requests.append((repo, issue_number))
        return IssueContext(self.title, self.body)

    async def get_text_file(self, repo, path, ref=None):
        self.file_requests.append((repo, path, ref))
        return self.agents

    async def dispatch_workflow(self, repo, workflow_id, ref, inputs=None):
        self.workflow_dispatches.append((repo, workflow_id, ref, inputs or {}))


class FakeDockhand:
    def __init__(self, volume_present=True):
        self.volume_present = volume_present
        self.volume_checks = []
        self.deploys = []
        self.pulls = []
        self.restores = []
        self.spawned = []
        self.destroyed = []
        self.url_by_repo = {}
        self.snapshot = ContainerSnapshot(
            "codex-runner",
            "zot.lan:5000/codex-runner:old",
            {"Config": {"Image": "zot.lan:5000/codex-runner:old"}, "State": {"Running": True}},
        )

    async def container_uses_volume(self, container, volume_name):
        self.volume_checks.append((container, volume_name))
        return self.volume_present

    async def spawn_runner(self, **kwargs):
        self.spawned.append(kwargs)
        repo = kwargs["environment"]["TASK_RUNNER_TARGET_REPO"]
        return self.url_by_repo.get(repo, f"http://{kwargs['name']}:7000")

    async def destroy_runner(self, name):
        self.destroyed.append(name)

    async def deploy_container_swap(self, stop_container, start_container):
        self.deploys.append((stop_container, start_container))
        return type(
            "ContainerDeployResult",
            (),
            {
                "stopped_container": stop_container,
                "started_container": start_container,
                "status": "running",
                "health_status": "healthy",
            },
        )()

    async def pull_image(self, image):
        self.pulls.append(image)

    async def snapshot_container(self, container):
        return self.snapshot

    async def replace_from_snapshot(self, snapshot, image):
        self.deploys.append((snapshot.name, snapshot.name))
        return type(
            "ContainerDeployResult",
            (),
            {"stopped_container": snapshot.name, "started_container": snapshot.name,
             "status": "running", "health_status": "healthy"},
        )()

    async def restore_snapshot(self, snapshot):
        self.restores.append(snapshot)
        return ContainerState(
            snapshot.name, "running", True, "healthy",
            {"Config": {"Image": snapshot.image}, "State": {"Running": True}},
        )


class FailingDeployDockhand(FakeDockhand):
    async def replace_from_snapshot(self, snapshot, image):
        self.deploys.append((snapshot.name, snapshot.name))
        raise RuntimeError("forced start failure")


class FakeRunner:
    def __init__(self, statuses=None, result=None, codex_version=None):
        self.statuses = iter(statuses or [{"status": "completed"}])
        self.result_data = result or {"result": "done", "log": "log output"}
        self.codex_version_data = codex_version or {
            "installed": "0.144.1",
            "latest": "0.144.1",
            "drift_detected": False,
        }
        self.cancelled = False
        self.prompt = None

    async def execute(self, url, repo, issue_number, prompt):
        self.prompt = prompt
        return "execution-1"

    async def status(self, url, execution_id):
        return next(self.statuses)

    async def result(self, url, execution_id):
        return self.result_data

    async def cancel(self, url, execution_id):
        self.cancelled = True
        return True

    async def codex_version(self, url):
        return self.codex_version_data


class BlockingRunner:
    def __init__(self):
        self.started = asyncio.Event()
        self.finish = asyncio.Event()
        self.executions = []

    async def execute(self, url, repo, issue_number, prompt):
        self.executions.append((url, repo, issue_number))
        self.started.set()
        return f"execution-{len(self.executions)}"

    async def status(self, url, execution_id):
        if not self.finish.is_set():
            return {"status": "running"}
        return {"status": "completed"}

    async def result(self, url, execution_id):
        return {"result": execution_id, "log": "done"}

    async def cancel(self, url, execution_id):
        return True

    async def codex_version(self, url):
        return {"installed": "0.144.1", "latest": "0.144.1", "drift_detected": False}


class PerRunnerStatusRunner:
    def __init__(self, terminal_status_by_url):
        self.terminal_status_by_url = terminal_status_by_url
        self.executions = []

    async def execute(self, url, repo, issue_number, prompt):
        self.executions.append((url, repo, issue_number))
        return f"{url}-execution-{len(self.executions)}"

    async def status(self, url, execution_id):
        return {"status": self.terminal_status_by_url[url]}

    async def result(self, url, execution_id):
        status = self.terminal_status_by_url[url]
        return {"result": status, "log": f"{url} log", "error": "injected failure" if status == "failed" else None}

    async def cancel(self, url, execution_id):
        return True

    async def codex_version(self, url):
        return {"installed": "0.144.1", "latest": "0.144.1", "drift_detected": False}


class SequencedPerRunnerStatusRunner(PerRunnerStatusRunner):
    def __init__(self, terminal_statuses_by_url):
        self.terminal_statuses_by_url = {
            url: iter(statuses) for url, statuses in terminal_statuses_by_url.items()
        }
        self.current_status_by_execution = {}
        self.executions = []

    async def execute(self, url, repo, issue_number, prompt):
        execution_id = f"{url}-execution-{len(self.executions) + 1}"
        self.executions.append((url, repo, issue_number))
        self.current_status_by_execution[execution_id] = next(self.terminal_statuses_by_url[url])
        return execution_id

    async def status(self, url, execution_id):
        return {"status": self.current_status_by_execution[execution_id]}

    async def result(self, url, execution_id):
        status = self.current_status_by_execution[execution_id]
        return {"result": status, "log": f"{url} log", "error": "injected failure" if status == "failed" else None}


class QuotaThenSuccessRunner:
    def __init__(self, resets_at):
        self.resets_at = resets_at
        self.executions = []

    async def execute(self, url, repo, issue_number, prompt):
        self.executions.append((url, repo, issue_number))
        return f"execution-{len(self.executions)}"

    async def resume(self, url, repo, issue_number, prompt, session_id):
        self.executions.append((url, repo, issue_number, session_id))
        return f"execution-{len(self.executions)}"

    async def status(self, url, execution_id):
        return {"status": "quota_exceeded" if execution_id == "execution-1" else "completed"}

    async def result(self, url, execution_id):
        if execution_id == "execution-1":
            return {
                "status": "quota_exceeded",
                "error": "Codex usage quota exceeded",
                "log": "quota exhausted",
                "resets_at": self.resets_at,
                "session_id": "session-1",
                "quota_auto_resume": True,
            }
        return {"status": "completed", "result": "done", "log": "completed after reset"}

    async def cancel(self, url, execution_id):
        return True


def make_service(tmp_path, runner, dockhand=None, **overrides):
    target = DeployTarget("app-dev", "app-dev-data", 8001)
    values = dict(
        database_path=str(tmp_path / "tasks.db"),
        runners={"codex": "http://runner"},
        repos=[
            RepoConfig("owner/repo", "codex", target, DeployTarget("app", "app-data", 8000)),
            RepoConfig(
                "owner/gemini", "gemini", target, DeployTarget("gemini", "gemini-data", 8002)
            ),
        ],
        poll_interval_seconds=0,
        github_token="test-token",
    )
    values.update(overrides)
    settings = Settings(**values)
    database = Database(settings.database_path)
    database.initialize()
    github = FakeGitHub()
    lifecycle = dockhand or FakeDockhand()
    if isinstance(lifecycle, FakeDockhand):
        lifecycle.url_by_repo = {
            repo.repo: settings.runners[repo.runner]
            for repo in settings.repos
            if repo.runner in settings.runners
        }
    service = TaskService(settings, database, {"github": github}, runner, lifecycle)
    service.fake_github = github
    return service


def test_interval_parser_accepts_seconds_minutes_hours_and_days():
    assert parse_interval_seconds("15") == 15
    assert parse_interval_seconds("2m") == 120
    assert parse_interval_seconds("1h") == 3600
    assert parse_interval_seconds("1d") == 86400


def test_interval_parser_rejects_non_positive_values():
    with pytest.raises(ValueError, match="greater than zero"):
        parse_interval_seconds("0s")


def test_settings_reads_dockhand_configuration(monkeypatch):
    monkeypatch.setenv("TASK_RUNNER_DOCKHAND_URL", "http://dockhand:3003")
    monkeypatch.setenv("TASK_RUNNER_DOCKHAND_TOKEN", "dh_test")
    monkeypatch.setenv("TASK_RUNNER_DOCKHAND_ENV", "2")
    monkeypatch.setenv("TASK_RUNNER_DOCKHAND_VERIFY_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("TASK_RUNNER_DOCKHAND_VERIFY_INTERVAL_SECONDS", "3")

    settings = Settings.from_env()

    assert settings.dockhand_url == "http://dockhand:3003"
    assert settings.dockhand_token == "dh_test"
    assert settings.dockhand_env == 2
    assert settings.dockhand_verify_timeout_seconds == 45
    assert settings.dockhand_verify_interval_seconds == 3


def test_settings_reads_configurable_global_container_cap(monkeypatch):
    monkeypatch.setenv("TASK_RUNNER_MAX_CONCURRENT_CONTAINERS", "2")

    assert Settings.from_env().max_concurrent_containers == 2
    assert Settings().max_concurrent_containers == 3


def test_settings_rejects_non_positive_global_container_cap():
    with pytest.raises(ValueError, match="MAX_CONCURRENT_CONTAINERS"):
        Settings(max_concurrent_containers=0)


def test_settings_reads_repo_registry(monkeypatch):
    monkeypatch.setenv(
        "TASK_RUNNER_REPOS",
        '[{"repo":"owner/repo","runner":"codex","dev":{"container":"app-dev","volume":"app-dev-data","port":8001,"health_path":"/health","expected_content":"ok"},"main":{"container":"app","volume":"app-data","port":8000,"human_promoted_only":true}}]',
    )

    settings = Settings.from_env()

    assert settings.repos == [
        RepoConfig(
            "owner/repo",
            "codex",
            DeployTarget("app-dev", "app-dev-data", 8001, "/health", "ok"),
            DeployTarget("app", "app-data", 8000, human_promoted_only=True),
        )
    ]


def test_repository_registry_seed_is_used_only_for_fresh_database(tmp_path):
    service = make_service(tmp_path, FakeRunner())
    service.initialize_repo_registry()
    replacement = [{
        "repo": "owner/persisted",
        "runner": "codex",
        "dev": {"container": "dev", "volume": "dev-data", "port": 1},
        "main": {"container": "main", "volume": "main-data", "port": 2},
    }]
    service.replace_repo_configs(replacement)

    restarted = make_service(tmp_path, FakeRunner(), repos=[])
    restarted.initialize_repo_registry()

    assert [repo["repo"] for repo in restarted.list_repo_configs()] == ["owner/persisted"]


def test_repo_registry_accepts_spawn_time_model_and_mcp_configuration(monkeypatch):
    monkeypatch.setenv(
        "TASK_RUNNER_REPOS",
        '[{"repo":"owner/repo","runner":"codex","model":"gpt-test",'
        '"mcp_servers":{"one":"http://one:7001/mcp"},'
        '"dev":{"container":"dev","volume":"dev-data","port":1},'
        '"main":{"container":"main","volume":"main-data","port":2}}]',
    )

    repo = Settings.from_env().repos[0]
    assert repo.model == "gpt-test"
    assert dict(repo.mcp_servers) == {"one": "http://one:7001/mcp"}


def test_repo_config_defaults_to_github_host():
    repo = RepoConfig.from_dict(
        {
            "repo": "owner/repo",
            "runner": "codex",
            "dev": {"container": "dev", "volume": "dev-data", "port": 1},
            "main": {"container": "main", "volume": "main-data", "port": 2},
        }
    )

    assert repo.host == "github"
    assert repo.host_base_url is None


def test_repo_config_accepts_forgejo_host_with_base_url():
    repo = RepoConfig.from_dict(
        {
            "repo": "owner/repo",
            "runner": "codex",
            "dev": {"container": "dev", "volume": "dev-data", "port": 1},
            "main": {"container": "main", "volume": "main-data", "port": 2},
            "host": "forgejo",
            "host_base_url": "https://forgejo.example.com",
        }
    )

    assert repo.host == "forgejo"
    assert repo.host_base_url == "https://forgejo.example.com"


def test_repo_config_rejects_forgejo_host_without_base_url():
    with pytest.raises(ValueError, match="host_base_url must be a non-empty string"):
        RepoConfig.from_dict(
            {
                "repo": "owner/repo",
                "runner": "codex",
                "dev": {"container": "dev", "volume": "dev-data", "port": 1},
                "main": {"container": "main", "volume": "main-data", "port": 2},
                "host": "forgejo",
            }
        )


def test_repo_config_rejects_invalid_host():
    with pytest.raises(ValueError, match="host must be github or forgejo"):
        RepoConfig.from_dict(
            {
                "repo": "owner/repo",
                "runner": "codex",
                "dev": {"container": "dev", "volume": "dev-data", "port": 1},
                "main": {"container": "main", "volume": "main-data", "port": 2},
                "host": "gitlab",
            }
        )


def test_deploy_registry_carries_ff_runner_configuration(monkeypatch):
    registry = Path("deploy/repos.json").read_text()
    monkeypatch.setenv("TASK_RUNNER_REPOS", registry)

    repos = {repo.repo: repo for repo in Settings.from_env().repos}
    expected_mcp_servers = {
        "ff-mcp": "http://192.168.1.68:7004/mcp",
        "nfl-mcp": "http://192.168.1.68:9000/mcp",
    }
    for name in ("m3rrym4n/fantasy-football-mcp-public", "m3rrym4n/nfl_mcp"):
        repo = repos[name]
        assert repo.model == "gpt-5.5"
        assert dict(repo.mcp_servers) == expected_mcp_servers
        assert repo.dev.container.startswith("NO_DEPLOY_TARGET_")
        assert repo.dev.human_promoted_only is True
        assert repo.main.human_promoted_only is True


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ('{}', "JSON array"),
        ('[{"repo":"owner/repo","runner":"codex","dev":{},"main":{}}]', "dev container"),
        ('[{"repo":"owner/repo","runner":"codex","dev":{"container":"dev","volume":"data","port":0},"main":{"container":"main","volume":"data","port":1}}]', "positive integer"),
    ],
)
def test_settings_rejects_malformed_repo_registry(monkeypatch, value, message):
    monkeypatch.setenv("TASK_RUNNER_REPOS", value)
    with pytest.raises(ValueError, match=message):
        Settings.from_env()


def test_settings_reads_ops_image_checks(monkeypatch):
    monkeypatch.setenv(
        "TASK_RUNNER_OPS_IMAGE_CHECKS",
        """[{"name":"codex","runner":"codex","repo":"owner/repo","issue_number":35,"registry":"zot.lan:5000","interval":"1d"}]""",
    )

    settings = Settings.from_env()

    assert settings.ops_image_checks == [
        OpsImageCheck(
            "codex",
            "codex",
            "owner/repo",
            35,
            "zot.lan:5000",
            "variflex-runner",
            "variflex-runner",
            "variflex-runner",
            "pacific-shift-codex-runner-auth",
            "unix:///run/buildkit/buildkitd.sock",
            "unknown",
            2,
            False,
            86400,
        )
    ]


@pytest.mark.asyncio
async def test_real_dispatch_lifecycle_and_tools(tmp_path):
    runner = FakeRunner(result={"result": "structured report", "log": "abc"})
    service = make_service(tmp_path, runner)
    receipt = await service.run_task("owner/repo", 2, "codex")
    task_id = receipt["task_id"]
    await asyncio.gather(*service._jobs)

    assert receipt["status"] == "running"
    assert receipt["position"] == 0
    assert receipt["queue_length"] == 1
    assert "instructions" in runner.prompt and "Test issue" in runner.prompt
    assert service.get_task_result(task_id)["result"] == "structured report"
    assert service.get_task_log(task_id)["log"] == "abc"
    assert service.list_tasks()[0]["status"] == "completed"
    assert len(service.dockhand.spawned) == 1
    assert service.dockhand.spawned[0]["auth_volume"] == "pacific-shift-codex-runner-auth"
    assert service.dockhand.destroyed == [service.dockhand.spawned[0]["name"]]


@pytest.mark.asyncio
async def test_dispatch_selects_forgejo_client_from_repo_host(tmp_path):
    target = DeployTarget("app-dev", "app-dev-data", 8001)
    repo = RepoConfig(
        "owner/repo",
        "codex",
        target,
        DeployTarget("app", "app-data", 8000),
        host="forgejo",
        host_base_url="https://forgejo.example.com",
    )
    runner = FakeRunner(result={"result": "structured report", "log": "abc"})
    service = make_service(tmp_path, runner, repos=[repo])
    github = FakeGitHub(title="Wrong host")
    forgejo = FakeGitHub(
        title="Forgejo issue", body="Forgejo acceptance criteria", agents="Forgejo instructions"
    )
    service.git_hosts = {"github": github, "forgejo": forgejo}

    await service.run_task("owner/repo", 118, "codex")
    await asyncio.gather(*service._jobs)

    assert github.context_requests == []
    assert forgejo.context_requests == [("owner/repo", 118)]
    assert forgejo.file_requests == [("owner/repo", "AGENTS.md", None)]
    assert "Forgejo instructions" in runner.prompt
    assert "Forgejo issue" in runner.prompt
    assert "https://forgejo.example.com/owner/repo" in runner.prompt
    assert "GitHub issue" not in runner.prompt
    environment = service.dockhand.spawned[0]["environment"]
    assert environment["TASK_RUNNER_GIT_HOST"] == "forgejo"
    assert environment["TASK_RUNNER_GIT_HOST_BASE_URL"] == "https://forgejo.example.com"


@pytest.mark.asyncio
async def test_spawn_environment_includes_github_token(tmp_path):
    runner = FakeRunner(result={"result": "structured report", "log": "abc"})
    service = make_service(tmp_path, runner)
    await service.run_task("owner/repo", 2, "codex")
    await asyncio.gather(*service._jobs)

    assert service.dockhand.spawned[0]["environment"]["GITHUB_TOKEN"] == "test-token"
    assert service.dockhand.spawned[0]["environment"]["TASK_RUNNER_GIT_HOST"] == "github"
    assert "TASK_RUNNER_GIT_HOST_BASE_URL" not in service.dockhand.spawned[0]["environment"]


@pytest.mark.asyncio
async def test_spawn_environment_omits_github_token_when_unconfigured(tmp_path):
    runner = FakeRunner(result={"result": "structured report", "log": "abc"})
    service = make_service(tmp_path, runner, github_token="")
    await service.run_task("owner/repo", 2, "codex")
    await asyncio.gather(*service._jobs)

    assert "GITHUB_TOKEN" not in service.dockhand.spawned[0]["environment"]


@pytest.mark.asyncio
async def test_timeout_is_persisted_and_cancellation_attempted(tmp_path):
    runner = FakeRunner(statuses=[{"status": "running"}] * 100)
    service = make_service(tmp_path, runner, timeout_seconds=0.01, poll_interval_seconds=0.005)
    receipt = await service.run_task("owner/repo", 2, "codex")
    task_id = receipt["task_id"]
    await asyncio.gather(*service._jobs)

    result = service.get_task_result(task_id)
    assert result["status"] == "timeout"
    assert runner.cancelled is True


@pytest.mark.asyncio
async def test_startup_resumes_running_task_and_records_completed_result(tmp_path):
    database_path = str(tmp_path / "tasks.db")
    settings = Settings(database_path=database_path, runners={"codex": "http://runner"}, poll_interval_seconds=0)
    database = Database(settings.database_path)
    database.initialize()
    database.create_task("task-1", "owner/repo", 2, "codex", "http://runner")
    database.update(
        "task-1",
        status="running",
        execution_id="execution-1",
        started_at="2026-07-11T13:22:16+00:00",
    )

    runner = FakeRunner(result={"result": "structured report", "log": "finished log"})
    service = TaskService(settings, database, {"github": FakeGitHub()}, runner)

    service.resume_running_tasks()
    await asyncio.gather(*service._jobs)

    result = service.get_task_result("task-1")
    assert result["status"] == "completed"
    assert result["result"] == "structured report"
    assert result["completed_at"] is not None
    assert service.get_task_log("task-1")["log"] == "finished log"


@pytest.mark.asyncio
async def test_startup_restores_pending_fifo_and_processes_in_order(tmp_path):
    service = make_service(tmp_path, FakeRunner())
    database = service.database
    database.create_task("task-1", "owner/repo", 2, "codex", "http://runner")
    database.create_task("task-2", "owner/repo", 3, "codex", "http://runner")
    database.save_runner_queue("codex", ["task-1", "task-2"], None, None, None, None)

    runner = SequencedPerRunnerStatusRunner({"http://runner": ["completed", "completed"]})
    dockhand = FakeDockhand()
    dockhand.url_by_repo = {"owner/repo": "http://runner"}
    restarted = TaskService(
        service.settings, database, {"github": FakeGitHub()}, runner, dockhand
    )
    restarted.resume_running_tasks()
    for _ in range(100):
        if restarted.get_task_result("task-2")["status"] == "completed":
            break
        await asyncio.sleep(0.001)

    assert [execution[2] for execution in runner.executions] == [2, 3]
    assert restarted.get_task_result("task-1")["status"] == "completed"
    assert restarted.get_task_result("task-2")["status"] == "completed"
    assert (await restarted.get_queue_states())["owner/repo"]["pending"] == []


@pytest.mark.asyncio
async def test_startup_restores_halt_state_and_does_not_process_pending(tmp_path):
    service = make_service(tmp_path, FakeRunner())
    service.database.create_task("task-1", "owner/repo", 2, "codex", "http://runner")
    service.database.save_runner_queue(
        "codex", ["task-1"], None, "halted", "previous task failed", None
    )

    runner = FakeRunner()
    restarted = TaskService(
        service.settings, service.database, {"github": FakeGitHub()}, runner
    )
    restarted.resume_running_tasks()
    await asyncio.gather(*restarted._jobs)

    queue = restarted._repo_queues["owner/repo"]
    assert list(queue.pending) == ["task-1"]
    assert queue.halt_state == "halted"
    assert queue.halt_reason == "previous task failed"
    assert runner.prompt is None


@pytest.mark.asyncio
async def test_startup_reconciles_active_remote_execution_without_redispatch(tmp_path):
    service = make_service(tmp_path, FakeRunner())
    service.database.create_task("active", "owner/repo", 2, "codex", "http://runner")
    service.database.update("active", status="running", execution_id="execution-1")
    service.database.create_task("pending", "owner/repo", 3, "codex", "http://runner")
    service.database.save_runner_queue("codex", ["pending"], "active", None, None, None)

    runner = SequencedPerRunnerStatusRunner({"http://runner": ["completed"]})
    runner.current_status_by_execution["execution-1"] = "completed"
    dockhand = FakeDockhand()
    dockhand.url_by_repo = {"owner/repo": "http://runner"}
    restarted = TaskService(
        service.settings,
        service.database,
        {"github": FakeGitHub()},
        runner,
        dockhand,
    )
    restarted.resume_running_tasks()
    await asyncio.gather(*restarted._jobs)

    assert [execution[2] for execution in runner.executions] == [3]
    assert restarted.get_task_result("active")["status"] == "completed"
    assert restarted.get_task_result("pending")["status"] == "completed"


@pytest.mark.asyncio
async def test_startup_fails_and_halts_active_task_without_execution_reference(tmp_path):
    service = make_service(tmp_path, FakeRunner())
    service.database.create_task("active", "owner/repo", 2, "codex", "http://runner")
    service.database.update("active", status="dispatching")
    service.database.create_task("pending", "owner/repo", 3, "codex", "http://runner")
    service.database.save_runner_queue("codex", ["pending"], "active", None, None, None)

    runner = FakeRunner()
    restarted = TaskService(
        service.settings, service.database, {"github": FakeGitHub()}, runner
    )
    restarted.resume_running_tasks()
    await asyncio.gather(*restarted._jobs)

    assert restarted.get_task_result("active")["status"] == "failed"
    assert restarted._repo_queues["owner/repo"].halt_state == "halted"
    assert list(restarted._repo_queues["owner/repo"].pending) == ["pending"]
    assert runner.prompt is None


@pytest.mark.asyncio
async def test_output_cap_is_visible(tmp_path):
    runner = FakeRunner(result={"result": "done", "log": "x" * 100})
    service = make_service(tmp_path, runner, output_cap_bytes=60)
    receipt = await service.run_task("owner/repo", 2, "codex")
    task_id = receipt["task_id"]
    await asyncio.gather(*service._jobs)

    log = service.get_task_log(task_id)
    assert log["output_truncated"] is True
    assert "OUTPUT TRUNCATED" in log["log"]
    assert len(log["log"].encode()) <= 60


@pytest.mark.asyncio
async def test_unknown_runner_is_rejected_without_persistence(tmp_path):
    service = make_service(tmp_path, FakeRunner())
    with pytest.raises(ValueError, match="Unknown runner"):
        await service.run_task("owner/repo", 2, "claude")
    assert service.list_tasks() == []


@pytest.mark.asyncio
async def test_unregistered_repo_is_rejected_without_persistence(tmp_path):
    service = make_service(tmp_path, FakeRunner())
    with pytest.raises(ValueError, match="not registered in TASK_RUNNER_REPOS"):
        await service.run_task("owner/missing", 2, "codex")
    assert service.list_tasks() == []


@pytest.mark.asyncio
async def test_repo_runner_mismatch_is_rejected_without_persistence(tmp_path):
    service = make_service(
        tmp_path,
        FakeRunner(),
        runners={"codex": "http://runner", "gemini": "http://gemini"},
    )
    with pytest.raises(ValueError, match="configured for runner 'codex'"):
        await service.run_task("owner/repo", 2, "gemini")
    assert service.list_tasks() == []


@pytest.mark.asyncio
async def test_scheduler_fires_configured_task_without_manual_dispatch(tmp_path):
    runner = FakeRunner()
    scheduled_task = ScheduledTask(
        name="health-check",
        repo="owner/repo",
        issue_number=2,
        runner="codex",
        interval_seconds=0.01,
    )
    service = make_service(tmp_path, runner, scheduled_tasks=[scheduled_task])

    service.start_scheduler()
    for _ in range(20):
        if service.list_tasks():
            break
        await asyncio.sleep(0.01)
    await service.stop_scheduler()
    await asyncio.gather(*service._jobs)

    tasks = service.list_tasks()
    assert len(tasks) >= 1
    assert tasks[0]["repo"] == "owner/repo"
    assert tasks[0]["issue_number"] == 2
    assert tasks[0]["repo"] == "owner/repo"
    assert tasks[0]["status"] == "completed"


@pytest.mark.asyncio
async def test_busy_runner_returns_queued_position_without_starting_second_task(tmp_path):
    runner = BlockingRunner()
    service = make_service(tmp_path, runner, poll_interval_seconds=0.001)

    first = await service.run_task("owner/repo", 2, "codex")
    await asyncio.wait_for(runner.started.wait(), timeout=1)
    second = await service.run_task("owner/repo", 3, "codex")

    assert first["status"] == "running"
    assert first["position"] == 0
    assert first["queue_length"] == 1
    assert second["status"] == "queued"
    assert second["position"] == 1
    assert second["queue_length"] == 2
    assert len(runner.executions) == 1

    runner.finish.set()
    await asyncio.gather(*service._jobs)
    assert len(runner.executions) == 2
    assert len(service.dockhand.spawned) == 2
    assert len(service.dockhand.destroyed) == 2


@pytest.mark.asyncio
async def test_failed_active_item_halts_runner_queue_and_logs(tmp_path, caplog):
    runner = PerRunnerStatusRunner({"http://runner": "failed"})
    service = make_service(tmp_path, runner)

    first = await service.run_task("owner/repo", 2, "codex")
    second = await service.run_task("owner/repo", 3, "codex")
    await asyncio.gather(*service._jobs)

    assert service.get_task_result(first["task_id"])["status"] == "failed"
    assert service.get_task_result(second["task_id"])["status"] == "queued"
    assert len(runner.executions) == 1
    assert "Repo queue 'owner/repo' halted" in caplog.text


@pytest.mark.asyncio
async def test_quota_halt_receipt_includes_resume_time_and_queue_auto_resumes(tmp_path):
    resets_at = (datetime.now(timezone.utc) + timedelta(seconds=0.5)).isoformat()
    runner = QuotaThenSuccessRunner(resets_at)
    service = make_service(tmp_path, runner, poll_interval_seconds=0.001)

    quota_task = await service.run_task("owner/repo", 2, "codex")
    for _ in range(100):
        if service._repo_queues["owner/repo"].halt_state == "quota_halted":
            break
        await asyncio.sleep(0.001)

    queued = await service.run_task("owner/repo", 3, "codex")

    quota_result = service.get_task_result(quota_task["task_id"])
    assert quota_result["status"] == "quota_exceeded"
    assert quota_result["resets_at"] == resets_at
    assert service._repo_queues["owner/repo"].halt_state == "quota_halted"
    assert service._repo_queues["owner/repo"].resumes_at == resets_at
    assert queued["status"] == "queued"
    assert queued["position"] == 1
    assert queued["queue_length"] == 2
    assert queued["resumes_at"] == resets_at
    assert len(runner.executions) == 1

    for _ in range(100):
        if service.get_task_result(queued["task_id"])["status"] == "completed":
            break
        await asyncio.sleep(0.005)

    assert service.get_task_result(queued["task_id"])["status"] == "completed"
    assert service.get_task_result(quota_task["task_id"])["status"] == "completed"
    assert service._repo_queues["owner/repo"].halt_state is None
    assert len(runner.executions) == 3
    assert runner.executions[1][3] == "session-1"
    assert len(service.dockhand.spawned) == 3
    assert len(service.dockhand.destroyed) == 3


@pytest.mark.asyncio
async def test_global_container_cap_queues_third_repo_until_slot_frees(tmp_path):
    runner = BlockingRunner()
    target = DeployTarget("dev", "dev-data", 8001)
    repos = [
        RepoConfig(f"owner/repo-{index}", "codex", target, DeployTarget("main", "data", 8000))
        for index in range(3)
    ]
    service = make_service(
        tmp_path,
        runner,
        repos=repos,
        max_concurrent_containers=2,
        poll_interval_seconds=0.001,
    )

    receipts = [
        await service.run_task(repo.repo, index + 1, "codex")
        for index, repo in enumerate(repos)
    ]
    for _ in range(100):
        if len(service.dockhand.spawned) == 2:
            break
        await asyncio.sleep(0.001)

    assert len(service.dockhand.spawned) == 2
    assert receipts[2]["status"] == "queued"
    assert service.get_task_result(receipts[2]["task_id"])["status"] == "queued"

    runner.finish.set()
    await asyncio.gather(*service._jobs)
    assert len(service.dockhand.spawned) == 3
    assert service.get_task_result(receipts[2]["task_id"])["status"] == "completed"


@pytest.mark.asyncio
async def test_phrasing_only_quota_detection_is_a_generic_halt(tmp_path):
    resets_at = (datetime.now(timezone.utc) + timedelta(seconds=0.01)).isoformat()
    runner = FakeRunner(
        result={
            "status": "quota_exceeded",
            "error": "Codex usage limit reached",
            "log": "You've hit your usage limit",
            "resets_at": resets_at,
            "session_id": "session-1",
            "quota_auto_resume": False,
        }
    )
    runner.statuses = iter([{"status": "quota_exceeded"}])
    service = make_service(tmp_path, runner)

    task = await service.run_task("owner/repo", 2, "codex")
    await asyncio.gather(*service._jobs)

    assert service.get_task_result(task["task_id"])["status"] == "quota_exceeded"
    assert service._repo_queues["owner/repo"].halt_state == "halted"


@pytest.mark.asyncio
async def test_generic_halt_does_not_auto_resume(tmp_path):
    runner = PerRunnerStatusRunner({"http://runner": "failed"})
    service = make_service(tmp_path, runner)

    failed = await service.run_task("owner/repo", 2, "codex")
    queued = await service.run_task("owner/repo", 3, "codex")
    await asyncio.gather(*service._jobs)
    await asyncio.sleep(0.05)

    assert service.get_task_result(failed["task_id"])["status"] == "failed"
    assert service.get_task_result(queued["task_id"])["status"] == "queued"
    assert service._repo_queues["owner/repo"].halt_state == "halted"
    assert "resumes_at" not in queued
    assert len(runner.executions) == 1


@pytest.mark.asyncio
async def test_halted_runner_queue_does_not_block_other_runner(tmp_path, caplog):
    runner = PerRunnerStatusRunner({"http://codex": "failed", "http://gemini": "completed"})
    service = make_service(
        tmp_path,
        runner,
        runners={"codex": "http://codex", "gemini": "http://gemini"},
    )

    failed = await service.run_task("owner/repo", 2, "codex")
    stuck = await service.run_task("owner/repo", 3, "codex")
    other = await service.run_task("owner/gemini", 4, "gemini")
    await asyncio.gather(*service._jobs)

    assert service.get_task_result(failed["task_id"])["status"] == "failed"
    assert service.get_task_result(stuck["task_id"])["status"] == "queued"
    assert service.get_task_result(other["task_id"])["status"] == "completed"
    assert [execution[0] for execution in runner.executions].count("http://codex") == 1
    assert [execution[0] for execution in runner.executions].count("http://gemini") == 1
    assert "Repo queue 'owner/repo' halted" in caplog.text


@pytest.mark.asyncio
async def test_clear_halt_resumes_only_selected_runner_without_retrying_failed_item(tmp_path):
    runner = SequencedPerRunnerStatusRunner(
        {"http://codex": ["failed", "completed"], "http://gemini": ["failed", "completed"]}
    )
    service = make_service(
        tmp_path,
        runner,
        runners={"codex": "http://codex", "gemini": "http://gemini"},
    )

    codex_failed = await service.run_task("owner/repo", 2, "codex")
    codex_pending = await service.run_task("owner/repo", 3, "codex")
    gemini_failed = await service.run_task("owner/gemini", 4, "gemini")
    gemini_pending = await service.run_task("owner/gemini", 5, "gemini")
    await asyncio.gather(*service._jobs)

    receipt = await service.clear_runner_halt("owner/repo")
    await asyncio.gather(*service._jobs)

    assert receipt == {
        "repo": "owner/repo",
        "status": "resumed",
        "previous_halt_state": "halted",
        "pending_count": 1,
    }
    assert service.get_task_result(codex_failed["task_id"])["status"] == "failed"
    assert service.get_task_result(codex_pending["task_id"])["status"] == "completed"
    assert service.get_task_result(gemini_failed["task_id"])["status"] == "failed"
    assert service.get_task_result(gemini_pending["task_id"])["status"] == "queued"
    assert service._repo_queues["owner/gemini"].halt_state == "halted"
    assert [execution[0] for execution in runner.executions].count("http://codex") == 2
    assert [execution[0] for execution in runner.executions].count("http://gemini") == 1


@pytest.mark.asyncio
async def test_cancel_queued_task_removes_it_and_it_is_never_processed(tmp_path):
    runner = BlockingRunner()
    service = make_service(tmp_path, runner, poll_interval_seconds=0.001)

    active = await service.run_task("owner/repo", 2, "codex")
    await asyncio.wait_for(runner.started.wait(), timeout=1)
    queued = await service.run_task("owner/repo", 3, "codex")

    receipt = await service.cancel_queued_task(queued["task_id"])
    runner.finish.set()
    await asyncio.gather(*service._jobs)

    assert receipt["status"] == "cancelled"
    assert receipt["pending_count"] == 0
    assert service.get_task_result(queued["task_id"])["status"] == "cancelled"
    assert service.get_task_result(active["task_id"])["status"] == "completed"
    assert len(runner.executions) == 1


@pytest.mark.asyncio
async def test_cancel_queued_task_rejects_active_item(tmp_path):
    runner = BlockingRunner()
    service = make_service(tmp_path, runner, poll_interval_seconds=0.001)

    active = await service.run_task("owner/repo", 2, "codex")
    await asyncio.wait_for(runner.started.wait(), timeout=1)

    with pytest.raises(ValueError, match="is active"):
        await service.cancel_queued_task(active["task_id"])

    runner.finish.set()
    await asyncio.gather(*service._jobs)
    assert service.get_task_result(active["task_id"])["status"] == "completed"


@pytest.mark.asyncio
async def test_ops_image_check_enqueues_issue_backed_rebuild_on_codex_version_drift(tmp_path):
    runner = FakeRunner(codex_version={"installed": "0.142.5", "latest": "0.144.1", "drift_detected": True})
    dockhand = FakeDockhand()
    service = make_service(tmp_path, runner, dockhand=dockhand)
    commands = []
    command_options = []

    async def fake_run_command(command, **kwargs):
        commands.append(command)
        command_options.append(kwargs)
        return "ok"

    service._run_command = fake_run_command
    check = OpsImageCheck(
        "codex",
        "codex",
        "owner/repo",
        35,
        "zot.lan:5000",
        "codex-runner",
        "codex-runner",
        "codex-runner",
        "pacific-shift-codex-runner-auth",
        "unix:///run/buildkit/buildkitd.sock",
        "abc1234",
        2,
        False,
        86400,
    )

    dispatched = await service.check_ops_image(check)
    await asyncio.gather(*service._jobs)

    assert dispatched is True
    assert service.fake_github.workflow_dispatches == []
    tasks = service.list_tasks()
    assert len(tasks) == 1
    assert tasks[0]["repo"] == "owner/repo"
    assert tasks[0]["issue_number"] == 35
    assert tasks[0]["status"] == "completed"
    assert commands[0][:7] == ["git", "clone", "--depth", "1", "--branch", "main", "--single-branch"]
    assert commands[0][7] == "https://github.com/m3rrym4n/variflex.git"
    assert command_options[0]["env"]["GIT_CONFIG_KEY_0"] == "http.https://github.com/.extraheader"
    assert "test-token" not in " ".join(commands[0])
    repo_dir = commands[0][8]
    source_dir = os.path.join(repo_dir, "runner")
    assert commands[1][:5] == ["buildctl", "--addr", "unix:///run/buildkit/buildkitd.sock", "build", "--frontend"]
    assert f"context={source_dir}" in commands[1]
    assert f"dockerfile={source_dir}" in commands[1]
    assert "context=/app/runner" not in commands[1]
    assert "dockerfile=/app/runner" not in commands[1]
    assert "type=image,name=zot.lan:5000/codex-runner:0.144.1-abc1234,push=true" in commands[1]
    assert commands[2][:4] == ["python", "/app/scripts/prune_zot_image_tags.py", "--registry", "https://zot.lan:5000"]
    assert not os.path.exists(repo_dir)
    assert dockhand.deploys == [("codex-runner", "codex-runner")]
    assert dockhand.pulls == ["zot.lan:5000/codex-runner:0.144.1-abc1234"]
    assert dockhand.volume_checks == [
        ("codex-runner", "pacific-shift-codex-runner-auth"),
        ("codex-runner", "pacific-shift-codex-runner-auth"),
    ]


@pytest.mark.asyncio
async def test_ops_image_forced_deploy_failure_restores_snapshot_and_logs_outcome(tmp_path):
    runner = FakeRunner(codex_version={"installed": "0.142.5", "latest": "0.144.1", "drift_detected": True})
    dockhand = FailingDeployDockhand()
    service = make_service(tmp_path, runner, dockhand=dockhand)
    service._run_command = lambda command, **kwargs: _async_value("ok")
    check = OpsImageCheck(
        "codex", "codex", "owner/repo", 54, "zot.lan:5000", "codex-runner",
        "codex-runner", "codex-runner", "pacific-shift-codex-runner-auth",
        "unix:///run/buildkit/buildkitd.sock", "abc1234", 2, False, 86400,
    )

    assert await service.check_ops_image(check) is True
    await asyncio.gather(*service._jobs)

    task = service.get_task_result(service.list_tasks()[0]["id"])
    log = service.get_task_log(task["id"])["log"]
    assert task["status"] == "failed"
    assert task["error"] == "RuntimeError: forced start failure"
    assert dockhand.restores == [dockhand.snapshot]
    assert "Rollback started" in log
    assert "Rollback verified from actual container state" in log


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_call", [1, 2, 3], ids=["clone", "build", "prune"])
async def test_ops_image_pre_deploy_failures_leave_container_untouched(tmp_path, failure_call):
    runner = FakeRunner(codex_version={"installed": "0.142.5", "latest": "0.144.1", "drift_detected": True})
    dockhand = FakeDockhand()
    service = make_service(tmp_path, runner, dockhand=dockhand)
    calls = 0

    commands = []

    async def fail_at_selected_command(command, **kwargs):
        nonlocal calls
        calls += 1
        commands.append(command)
        if calls == failure_call:
            raise RuntimeError("forced pre-deploy failure")
        return "ok"

    service._run_command = fail_at_selected_command
    check = OpsImageCheck(
        "codex", "codex", "owner/repo", 54, "zot.lan:5000", "codex-runner",
        "codex-runner", "codex-runner", "pacific-shift-codex-runner-auth",
        "unix:///run/buildkit/buildkitd.sock", "abc1234", 2, False, 86400,
    )

    assert await service.check_ops_image(check) is True
    await asyncio.gather(*service._jobs)

    assert service.list_tasks()[0]["status"] == "failed"
    assert dockhand.pulls == []
    assert dockhand.deploys == []
    assert dockhand.restores == []
    source_dir = commands[0][-1]
    assert not os.path.exists(os.path.dirname(source_dir))


async def _async_value(value):
    return value


@pytest.mark.asyncio
async def test_regular_dispatch_queues_behind_active_ops_rebuild(tmp_path):
    runner = FakeRunner(codex_version={"installed": "0.142.5", "latest": "0.144.1", "drift_detected": True})
    service = make_service(tmp_path, runner, dockhand=FakeDockhand(), poll_interval_seconds=0.001)
    rebuild_started = asyncio.Event()
    finish_rebuild = asyncio.Event()

    async def fake_run_command(command, **kwargs):
        rebuild_started.set()
        await finish_rebuild.wait()
        return "ok"

    service._run_command = fake_run_command
    check = OpsImageCheck(
        "codex",
        "codex",
        "owner/repo",
        35,
        "zot.lan:5000",
        "codex-runner",
        "codex-runner",
        "codex-runner",
        "pacific-shift-codex-runner-auth",
        "unix:///run/buildkit/buildkitd.sock",
        "abc1234",
        2,
        False,
        86400,
    )

    assert await service.check_ops_image(check) is True
    await asyncio.wait_for(rebuild_started.wait(), timeout=1)
    queued = await service.run_task("owner/repo", 36, "codex")

    assert queued["status"] == "queued"
    assert queued["position"] == 1
    assert runner.prompt is None

    finish_rebuild.set()
    await asyncio.gather(*service._jobs)

    assert service.list_tasks()[0]["issue_number"] == 36
    assert service.list_tasks()[0]["status"] == "completed"


@pytest.mark.asyncio
async def test_ops_image_check_skips_workflow_when_versions_match(tmp_path):
    service = make_service(tmp_path, FakeRunner())
    check = OpsImageCheck(
        "codex",
        "codex",
        "owner/repo",
        35,
        "zot.lan:5000",
        "codex-runner",
        "codex-runner",
        "codex-runner",
        "pacific-shift-codex-runner-auth",
        "unix:///run/buildkit/buildkitd.sock",
        "abc1234",
        2,
        False,
        86400,
    )

    dispatched = await service.check_ops_image(check)

    assert dispatched is False
    assert service.fake_github.workflow_dispatches == []


@pytest.mark.asyncio
async def test_ops_image_check_falls_back_to_version_comparison_for_non_boolean_drift_flag(tmp_path):
    runner = FakeRunner(codex_version={"installed": "0.144.1", "latest": "0.144.1", "drift_detected": "false"})
    service = make_service(tmp_path, runner)
    check = OpsImageCheck(
        "codex",
        "codex",
        "owner/repo",
        35,
        "zot.lan:5000",
        "codex-runner",
        "codex-runner",
        "codex-runner",
        "pacific-shift-codex-runner-auth",
        "unix:///run/buildkit/buildkitd.sock",
        "abc1234",
        2,
        False,
        86400,
    )

    dispatched = await service.check_ops_image(check)

    assert dispatched is False
    assert service.fake_github.workflow_dispatches == []
