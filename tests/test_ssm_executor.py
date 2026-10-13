from unittest.mock import MagicMock, patch
from src.config import WorkerNode
from src.ssm_executor import SSMExecutor

def test_ssm_executor_success():
    worker = WorkerNode(
        name="Worker-01",
        region="eu-north-1",
        instance_id="i-test12345"
    )

    mock_ssm = MagicMock()
    mock_ssm.send_command.return_value = {"Command": {"CommandId": "cmd-abc-123"}}
    mock_ssm.get_command_invocation.return_value = {
        "Status": "Success",
        "ResponseCode": 0,
        "StandardOutputContent": "Pipeline finished successfully\n",
        "StandardErrorContent": ""
    }

    executor = SSMExecutor()
    executor._clients["eu-north-1"] = mock_ssm

    res = executor.run_command(worker, ["echo 'Hello Cloud'"], timeout_sec=10)
    assert res.is_success
    assert res.exit_code == 0
    assert "Pipeline finished" in res.stdout
    assert res.command_id == "cmd-abc-123"


@patch("src.ssm_executor.time.sleep")
def test_ssm_poll_backs_off(sleep):
    worker = WorkerNode(name="W", region="eu-north-1", instance_id="i-1")
    ssm = MagicMock()
    ssm.send_command.return_value = {"Command": {"CommandId": "c"}}
    ssm.get_command_invocation.side_effect = [
        {"Status": "InProgress"}, {"Status": "InProgress"}, {"Status": "InProgress"},
        {"Status": "Success", "ResponseCode": 0},
    ]
    ex = SSMExecutor()
    ex._clients["eu-north-1"] = ssm
    assert ex.run_command(worker, ["x"], timeout_sec=60).is_success
    delays = [c.args[0] for c in sleep.call_args_list]
    assert delays == [1.0, 2.0, 4.0, 8.0]
