from unittest.mock import MagicMock, patch
import pytest
from botocore.exceptions import ClientError
from src.config import WorkerNode
from src.fleet_manager import FleetManager

W = WorkerNode(name="W1", region="eu-north-1", instance_id="i-1")


def _err(code):
    return ClientError({"Error": {"Code": code, "Message": "x"}}, "Op")


def _fm(ec2=None, ssm=None):
    fm = FleetManager()
    fm._clients["eu-north-1"] = ec2 or MagicMock()
    fm._clients["ssm_eu-north-1"] = ssm or MagicMock()
    return fm


def _state(name):
    return {"Reservations": [{"Instances": [{"State": {"Name": name}}]}]}


@patch("src.fleet_manager.time.sleep")
def test_stop_retries_then_succeeds(_):
    ec2 = MagicMock()
    ec2.stop_instances.side_effect = [_err("Throttling"), None]
    assert _fm(ec2).stop_worker(W) is True
    assert ec2.stop_instances.call_count == 2


@patch("src.fleet_manager.time.sleep")
def test_stop_returns_false_after_all_attempts_fail(_):
    ec2 = MagicMock()
    ec2.stop_instances.side_effect = _err("UnauthorizedOperation")
    assert _fm(ec2).stop_worker(W, attempts=3) is False
    assert ec2.stop_instances.call_count == 3


@patch("src.fleet_manager.time.sleep")
def test_start_while_stopping_waits_then_restarts(_):
    ec2 = MagicMock()
    ec2.start_instances.side_effect = [_err("IncorrectInstanceState"), None]
    ec2.describe_instances.side_effect = [_state("stopping"), _state("stopping"), _state("stopped")]
    assert _fm(ec2).start_worker(W, wait_for_online=False) is True
    assert ec2.start_instances.call_count == 2


def test_start_already_running_is_not_restarted():
    ec2 = MagicMock()
    ec2.start_instances.side_effect = _err("IncorrectInstanceState")
    ec2.describe_instances.return_value = _state("running")
    assert _fm(ec2).start_worker(W, wait_for_online=False) is True
    assert ec2.start_instances.call_count == 1


def test_start_other_error_raises():
    ec2 = MagicMock()
    ec2.start_instances.side_effect = _err("UnauthorizedOperation")
    with pytest.raises(ClientError):
        _fm(ec2).start_worker(W, wait_for_online=False)


def test_start_in_terminated_state_raises():
    ec2 = MagicMock()
    ec2.start_instances.side_effect = _err("IncorrectInstanceState")
    ec2.describe_instances.return_value = _state("terminated")
    with pytest.raises(ClientError):
        _fm(ec2).start_worker(W, wait_for_online=False)


@patch("src.fleet_manager.time.sleep")
def test_ssm_wait_backs_off_and_times_out(sleep):
    ssm = MagicMock()
    ssm.describe_instance_information.return_value = {"InstanceInformationList": []}
    clock = iter(range(0, 1000, 20))
    with patch("src.fleet_manager.time.time", lambda: next(clock)):
        assert _fm(ssm=ssm).wait_for_ssm_ready(W, timeout_sec=100) is False
    delays = [c.args[0] for c in sleep.call_args_list]
    assert delays == sorted(delays) and delays[-1] > delays[0]
