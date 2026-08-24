import asyncio
from collections import deque
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock

from fastapi.testclient import TestClient

from task_runner import main
from task_runner.service import RunnerQueue
from tests.test_service import FakeRunner, PerRunnerStatusRunner, make_service


def create_task(service, task_id, issue_number, created_at, status="completed", **values):
    service.database.create_task(task_id, "owner/repo", issue_number, "codex", "http://runner")
    service.database.update(task_id, created_at=created_at, status=status, **values)


def test_tasks_endpoint_filters_paginates_and_returns_public_fields(tmp_path, monkeypatch):
    service = make_service(tmp_path, FakeRunner())
    now = datetime.now(timezone.utc)
    create_task(
        service,
        "newest",
        23,
        now.isoformat(),
        status="completed",
        completed_at=now.isoformat(),
        branch="main",
        pr_url="https://github.com/owner/repo/pull/1",
        session_id="session-1",
    )
    create_task(service, "running", 22, (now - timedelta(hours=1)).isoformat(), status="queued")
    create_task(service, "old", 21, (now - timedelta(days=2)).isoformat(), status="failed")
    monkeypatch.setattr(main, "service", service)
    client = TestClient(main.app)

    response = client.get("/api/tasks", params={"window": "7d", "limit": 1, "offset": 1})

    assert response.status_code == 200
    assert response.json() == {
        "tasks": [
            {
                "id": "running",
                "repo": "owner/repo",
                "issue_number": 22,
                "runner": "codex",
                "status": "running",
                "created_at": (now - timedelta(hours=1)).isoformat(),
                "completed_at": None,
                "branch": None,
                "pr_url": None,
                "resets_at": None,
                "session_id": None,
            }
        ],
        "running_count": 1,
    }
    assert [task["id"] for task in client.get("/api/tasks").json()["tasks"]] == ["newest", "running"]
    assert [task["id"] for task in client.get("/api/tasks?window=all").json()["tasks"]] == [
        "newest", "running", "old"
    ]
    assert client.get("/api/tasks?window=1h").status_code == 422
    assert client.get("/api/tasks?limit=0").status_code == 422


def test_health_and_mcp_routes_remain_available(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "service", make_service(tmp_path, FakeRunner()))
    client = TestClient(main.app)

    assert client.get("/").json() == {"service": "variflex", "status": "ok"}
    assert any(getattr(route, "path", None) == "/mcp" for route in main.app.routes)


def test_run_task_endpoint_dispatches_and_validates_errors(monkeypatch):
    receipt = {
        "task_id": "task-1",
        "status": "queued",
        "position": 1,
        "queue_length": 2,
        "runner": "codex",
    }
    rest_service = Mock()
    rest_service.run_task = AsyncMock(return_value=receipt)
    monkeypatch.setattr(main, "service", rest_service)
    client = TestClient(main.app)

    response = client.post(
        "/api/tasks", json={"repo": "owner/repo", "issue_number": 48, "runner": "codex"}
    )

    assert response.status_code == 202
    assert response.json() == receipt
    rest_service.run_task.assert_awaited_once_with("owner/repo", 48, "codex")
    assert client.post("/api/tasks", json={"repo": "owner/repo"}).status_code == 422

    rest_service.run_task.side_effect = ValueError("Unknown runner 'other'")
    error_response = client.post(
        "/api/tasks", json={"repo": "owner/repo", "issue_number": 48, "runner": "other"}
    )
    assert error_response.status_code == 400
    assert error_response.json() == {"detail": "Unknown runner 'other'"}


def test_task_result_endpoint_matches_service_response_and_returns_404(monkeypatch):
    result = {"id": "task-1", "status": "completed", "result": "done"}
    rest_service = Mock()
    rest_service.get_task_result.return_value = result
    monkeypatch.setattr(main, "service", rest_service)
    client = TestClient(main.app)

    response = client.get("/api/tasks/task-1")

    assert response.status_code == 200
    assert response.json() == result
    rest_service.get_task_result.assert_called_once_with("task-1")
    rest_service.get_task_result.side_effect = ValueError("Unknown task_id: missing")
    assert client.get("/api/tasks/missing").status_code == 404


def test_task_log_endpoint_matches_service_response_and_returns_404(monkeypatch):
    log = {"id": "task-1", "status": "running", "log": "output", "output_truncated": False}
    rest_service = Mock()
    rest_service.get_task_log.return_value = log
    monkeypatch.setattr(main, "service", rest_service)
    client = TestClient(main.app)

    response = client.get("/api/tasks/task-1/log")

    assert response.status_code == 200
    assert response.json() == log
    rest_service.get_task_log.assert_called_once_with("task-1")
    rest_service.get_task_log.side_effect = ValueError("Unknown task_id: missing")
    assert client.get("/api/tasks/missing/log").status_code == 404


def test_cancel_task_endpoint_matches_service_response_and_maps_errors(monkeypatch):
    cancelled = {
        "task_id": "task-1",
        "runner": "codex",
        "status": "cancelled",
        "pending_count": 0,
    }
    rest_service = Mock()
    rest_service.cancel_queued_task = AsyncMock(return_value=cancelled)
    monkeypatch.setattr(main, "service", rest_service)
    client = TestClient(main.app)

    response = client.post("/api/tasks/task-1/cancel")

    assert response.status_code == 200
    assert response.json() == cancelled
    rest_service.cancel_queued_task.assert_awaited_once_with("task-1")
    rest_service.cancel_queued_task.side_effect = ValueError("Task 'task-1' is active")
    assert client.post("/api/tasks/task-1/cancel").status_code == 409
    rest_service.cancel_queued_task.side_effect = ValueError("Unknown task_id: missing")
    assert client.post("/api/tasks/missing/cancel").status_code == 404


def test_clear_runner_halt_endpoint_matches_service_response_and_returns_404(monkeypatch):
    cleared = {
        "repo": "owner/repo",
        "status": "resumed",
        "previous_halt_state": "halted",
        "pending_count": 1,
    }
    rest_service = Mock()
    rest_service.clear_runner_halt = AsyncMock(return_value=cleared)
    monkeypatch.setattr(main, "service", rest_service)
    client = TestClient(main.app)

    response = client.post("/api/queues/owner/repo/clear-halt")

    assert response.status_code == 200
    assert response.json() == cleared
    rest_service.clear_runner_halt.assert_awaited_once_with("owner/repo")
    rest_service.clear_runner_halt.side_effect = ValueError("Unknown repo 'other/repo'")
    assert client.post("/api/queues/other/repo/clear-halt").status_code == 404


def test_repo_registry_endpoint_returns_deploy_targets(tmp_path, monkeypatch):
    monkeypatch.setattr(main, "service", make_service(tmp_path, FakeRunner()))
    response = TestClient(main.app).get("/api/repos")
    assert response.status_code == 200
    assert response.json()["repos"][0]["repo"] == "owner/repo"
    assert response.json()["repos"][0]["dev"]["container"] == "app-dev"


def test_repo_registry_put_replaces_live_dispatch_registry(tmp_path, monkeypatch):
    service = make_service(tmp_path, FakeRunner())
    monkeypatch.setattr(main, "service", service)
    client = TestClient(main.app)
    replacement = [{
        "repo": "owner/new",
        "runner": "codex",
        "dev": {"container": "new-dev", "volume": "new-dev-data", "port": 8101},
        "main": {"container": "new", "volume": "new-data", "port": 8100},
    }]

    response = client.put("/api/repos", json=replacement)

    assert response.status_code == 200
    assert client.get("/api/repos").json()["repos"][0]["repo"] == "owner/new"
    assert asyncio.run(service.run_task("owner/new", 91, "codex"))["runner"] == "codex"
    try:
        asyncio.run(service.run_task("owner/repo", 91, "codex"))
    except ValueError as exc:
        assert "not registered" in str(exc)
    else:
        raise AssertionError("removed repository remained dispatchable")


def test_repo_registry_put_rejects_invalid_list_without_changing_store(tmp_path, monkeypatch):
    service = make_service(tmp_path, FakeRunner())
    service.initialize_repo_registry()
    monkeypatch.setattr(main, "service", service)
    client = TestClient(main.app)
    before = client.get("/api/repos").json()

    response = client.put("/api/repos", json=[{
        "repo": "owner/broken",
        "runner": "codex",
        "dev": {"container": "broken-dev", "volume": "data"},
        "main": {"container": "broken", "volume": "data", "port": 8000},
    }])

    assert response.status_code == 400
    assert response.json() == {"detail": "repo dev port must be a positive integer"}
    assert client.get("/api/repos").json() == before
    assert service.database.load_repo_configs() == before["repos"]


async def wait_for_halt(service):
    for _ in range(100):
        queue = service._repo_queues["owner/repo"]
        if queue.halt_state == "halted":
            return
        await asyncio.sleep(0.001)
    raise AssertionError("runner queue did not halt")


def test_queues_endpoint_reports_real_pending_position_and_halt_without_side_effects(
    tmp_path, monkeypatch
):
    async def arrange():
        service = make_service(
            tmp_path,
            PerRunnerStatusRunner({"http://runner": "failed"}),
            runners={"codex": "http://runner", "idle": "http://idle"},
        )
        first = await service.run_task("owner/repo", 1, "codex")
        second = await service.run_task("owner/repo", 2, "codex")
        third = await service.run_task("owner/repo", 3, "codex")
        await asyncio.gather(*service._jobs)
        await wait_for_halt(service)
        return service, first, second, third

    service, first, second, third = asyncio.run(arrange())
    monkeypatch.setattr(main, "service", service)
    before_tasks = service.database.list()
    client = TestClient(main.app)

    first_response = client.get("/api/queues")
    second_response = client.get("/api/queues")

    assert first_response.status_code == 200
    assert first_response.json() == second_response.json() == {
        "owner/repo": {
            "active_task_id": None,
            "pending": [second["task_id"], third["task_id"]],
            "halt_state": "halted",
            "resumes_at": None,
        },
        "owner/gemini": {
            "active_task_id": None,
            "pending": [],
            "halt_state": None,
            "resumes_at": None,
        },
    }
    assert service.get_task_result(first["task_id"])["status"] == "failed"
    assert service.database.list() == before_tasks


def test_queues_endpoint_reports_quota_halt_state(tmp_path, monkeypatch):
    service = make_service(tmp_path, FakeRunner())
    service._repo_queues["owner/repo"] = RunnerQueue(
        pending=deque(["quota-task", "next-task"]),
        halt_state="quota_halted",
        resumes_at="2026-07-13T01:00:00+00:00",
    )
    monkeypatch.setattr(main, "service", service)

    assert TestClient(main.app).get("/api/queues").json()["owner/repo"] == {
        "active_task_id": None,
        "pending": ["quota-task", "next-task"],
        "halt_state": "quota_halted",
        "resumes_at": "2026-07-13T01:00:00+00:00",
    }
