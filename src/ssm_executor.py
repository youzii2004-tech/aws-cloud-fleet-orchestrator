"""
ssm_executor.py — Remote job dispatcher via AWS Systems Manager (SSM) RunCommand.
Dispatches commands securely over the AWS control plane without opening port 22 (SSH)
or exposing public network interfaces.
"""
import time
import logging
from typing import Dict, Any, Optional, List
import boto3
from botocore.exceptions import ClientError
from .config import WorkerNode

logger = logging.getLogger("SSMExecutor")


class ExecutionResult:
    def __init__(self, command_id: str, status: str, exit_code: int, stdout: str, stderr: str, elapsed_sec: float):
        self.command_id = command_id
        self.status = status
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.elapsed_sec = elapsed_sec

    @property
    def is_success(self) -> bool:
        return self.status == "Success" and self.exit_code == 0

    def __repr__(self) -> str:
        return f"<ExecutionResult id={self.command_id} status={self.status} exit_code={self.exit_code} time={self.elapsed_sec:.1f}s>"


class SSMExecutor:
    def __init__(self, aws_profile: Optional[str] = None):
        self.aws_profile = aws_profile
        self._clients: Dict[str, Any] = {}

    def _get_ssm(self, region: str):
        if region not in self._clients:
            session = boto3.Session(profile_name=self.aws_profile, region_name=region)
            self._clients[region] = session.client("ssm")
        return self._clients[region]

    def run_command(
        self,
        worker: WorkerNode,
        commands: List[str],
        timeout_sec: int = 900,
        comment: str = "Fleet Task"
    ) -> ExecutionResult:
        """Dispatches an array of shell commands via AWS-RunShellScript and polls to completion."""
        ssm = self._get_ssm(worker.region)
        start_time = time.time()

        logger.info(f"[{worker.name}] Dispatching {len(commands)} command(s) via SSM RunCommand...")
        try:
            resp = ssm.send_command(
                InstanceIds=[worker.instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={
                    "commands": commands,
                    "executionTimeout": [str(timeout_sec)]
                },
                Comment=comment,
                TimeoutSeconds=timeout_sec
            )
            command_id = resp["Command"]["CommandId"]
        except ClientError as e:
            logger.error(f"[{worker.name}] Failed to dispatch SSM command: {e}")
            return ExecutionResult("", "FailedToDispatch", -1, "", str(e), 0.0)

        logger.info(f"[{worker.name}] Command dispatched. Command ID: {command_id}. Polling execution...")

        # Poll loop
        while time.time() - start_time < timeout_sec:
            time.sleep(3.0)
            try:
                inv = ssm.get_command_invocation(
                    CommandId=command_id,
                    InstanceId=worker.instance_id
                )
                status = inv.get("Status", "Pending")
                if status in ("Success", "Failed", "Cancelled", "TimedOut"):
                    elapsed = time.time() - start_time
                    exit_code = inv.get("ResponseCode", 0 if status == "Success" else 1)
                    stdout = inv.get("StandardOutputContent", "")
                    stderr = inv.get("StandardErrorContent", "")
                    logger.info(f"[{worker.name}] Command {command_id} finished with status '{status}' (code {exit_code}) in {elapsed:.1f}s.")
                    return ExecutionResult(command_id, status, exit_code, stdout, stderr, elapsed)
            except ClientError as e:
                # In rare cases, invocation isn't indexed immediately
                if "InvocationDoesNotExist" in str(e):
                    continue
                logger.warning(f"[{worker.name}] Polling warning: {e}")

        elapsed = time.time() - start_time
        logger.error(f"[{worker.name}] Command {command_id} exceeded client timeout of {timeout_sec}s.")
        return ExecutionResult(command_id, "ClientTimedOut", -1, "", "Execution timed out", elapsed)
