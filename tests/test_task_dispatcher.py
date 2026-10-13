import threading
import time
from unittest.mock import MagicMock, patch
import pytest
from src.config import FleetConfig, WorkerNode, StorageConfig, ExecutionPolicy
from src.task_dispatcher import TaskDispatcher, FleetTask


def _dispatcher(workers, max_parallel=5):
    cfg = FleetConfig(
        workers=workers,
        storage=StorageConfig(bucket_name="b"),
        execution=ExecutionPolicy(max_parallel_workers=max_parallel),
    )
    with patch("src.task_dispatcher.S3SyncManager"):
        d = TaskDispatcher(cfg)
    d.fleet_manager = MagicMock()
    d.fleet_manager.start_worker.return_value = True
    d.fleet_manager.stop_worker.return_value = True
    d.ssm_executor = MagicMock()
    d.ssm_executor.run_command.return_value = MagicMock(is_success=True)
    return d


W1 = WorkerNode(name="W1", region="eu-north-1", instance_id="i-1")


def test_shared_worker_stopped_only_after_last_task():
    d = _dispatcher([W1])
    stops_at_run_end = []
    active = {"n": 0}
    lock = threading.Lock()

    def run(worker, commands, timeout_sec, comment):
        with lock:
            active["n"] += 1
        time.sleep(0.15 if comment.endswith("slow") else 0.01)
        with lock:
            active["n"] -= 1
        return MagicMock(is_success=True)

    def stop(worker):
        stops_at_run_end.append(active["n"])
        return True

    d.ssm_executor.run_command.side_effect = run
    d.fleet_manager.stop_worker.side_effect = stop
    res = d.dispatch_batch([FleetTask("fast", ["x"]), FleetTask("slow", ["x"])])

    assert [r["status"] for r in res] == ["Success", "Success"]
    assert d.fleet_manager.stop_worker.call_count == 1
    assert stops_at_run_end == [0]  # nothing was still running when it was stopped


def test_stop_failure_is_reported():
    d = _dispatcher([W1])
    d.fleet_manager.stop_worker.return_value = False
    res = d.dispatch_batch([FleetTask("t", ["x"])])
    assert res[0]["stop_failed"] is True


def test_instance_stopped_even_when_command_raises():
    d = _dispatcher([W1])
    d.ssm_executor.run_command.side_effect = RuntimeError("boom")
    res = d.dispatch_batch([FleetTask("t", ["x"])])
    assert res[0]["status"] == "Error"
    d.fleet_manager.stop_worker.assert_called_once()


def test_instance_stopped_when_start_fails():
    d = _dispatcher([W1])
    d.fleet_manager.start_worker.return_value = False
    res = d.dispatch_batch([FleetTask("t", ["x"])])
    assert res[0]["status"] == "Failed"
    d.ssm_executor.run_command.assert_not_called()
    d.fleet_manager.stop_worker.assert_called_once()


def test_auto_stop_false_never_stops():
    w = WorkerNode(name="W2", region="r", instance_id="i-2", auto_stop=False)
    d = _dispatcher([w])
    d.dispatch_batch([FleetTask("t", ["x"])])
    d.fleet_manager.stop_worker.assert_not_called()


def test_unknown_target_worker_raises():
    d = _dispatcher([W1])
    with pytest.raises(ValueError):
        d.dispatch_batch([FleetTask("t", ["x"], target_worker_name="nope")])


def test_batch_can_rerun_after_completion():
    d = _dispatcher([W1])
    d.dispatch_batch([FleetTask("a", ["x"])])
    d.dispatch_batch([FleetTask("b", ["x"])])
    assert d.fleet_manager.stop_worker.call_count == 2
