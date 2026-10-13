"""
fleet_manager.py — EC2 multi-region instance lifecycle manager.
Enforces zero-idle cost policy: instances are booted on-demand before execution
and automatically transitioned to 'stopped' state upon task completion or failure.
"""
import time
import logging
from typing import Dict, Any, Optional
import boto3
from botocore.exceptions import ClientError
from .config import WorkerNode

logger = logging.getLogger("FleetManager")


class FleetManager:
    def __init__(self, aws_profile: Optional[str] = None):
        self.aws_profile = aws_profile
        self._clients: Dict[str, Any] = {}

    def _get_ec2(self, region: str):
        if region not in self._clients:
            session = boto3.Session(profile_name=self.aws_profile, region_name=region)
            self._clients[region] = session.client("ec2")
        return self._clients[region]

    def _get_ssm(self, region: str):
        key = f"ssm_{region}"
        if key not in self._clients:
            session = boto3.Session(profile_name=self.aws_profile, region_name=region)
            self._clients[key] = session.client("ssm")
        return self._clients[key]

    def _instance_state(self, worker: WorkerNode) -> str:
        resp = self._get_ec2(worker.region).describe_instances(InstanceIds=[worker.instance_id])
        return resp["Reservations"][0]["Instances"][0]["State"]["Name"]

    def _wait_for_state(self, worker: WorkerNode, target: str, timeout_sec: int = 300) -> bool:
        deadline = time.time() + timeout_sec
        delay = 2.0
        while time.time() < deadline:
            if self._instance_state(worker) == target:
                return True
            time.sleep(delay)
            delay = min(delay * 2, 15.0)
        return False

    def start_worker(self, worker: WorkerNode, wait_for_online: bool = True) -> bool:
        """Starts an EC2 instance and optionally awaits SSM agent readiness."""
        ec2 = self._get_ec2(worker.region)
        logger.info(f"[{worker.name}] Starting instance {worker.instance_id} in {worker.region}...")
        try:
            ec2.start_instances(InstanceIds=[worker.instance_id])
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "IncorrectInstanceState":
                logger.error(f"[{worker.name}] Failed to start instance: {e}")
                raise
            state = self._instance_state(worker)
            if state in ("pending", "running"):
                logger.warning(f"[{worker.name}] Instance already {state}.")
            elif state in ("stopping", "stopped"):
                # Mid-stop: let the stop finish, then start again.
                logger.warning(f"[{worker.name}] Instance is {state}; waiting for 'stopped' before starting.")
                if not self._wait_for_state(worker, "stopped"):
                    logger.error(f"[{worker.name}] Instance never reached 'stopped'.")
                    return False
                ec2.start_instances(InstanceIds=[worker.instance_id])
            else:
                logger.error(f"[{worker.name}] Cannot start instance in state '{state}'.")
                raise

        if wait_for_online:
            return self.wait_for_ssm_ready(worker)
        return True

    def stop_worker(self, worker: WorkerNode, attempts: int = 3) -> bool:
        """Stops an EC2 instance to halt compute charges. Retries; returns False if all attempts fail."""
        ec2 = self._get_ec2(worker.region)
        logger.info(f"[{worker.name}] Stopping instance {worker.instance_id}...")
        for attempt in range(1, attempts + 1):
            try:
                ec2.stop_instances(InstanceIds=[worker.instance_id])
                logger.info(f"[{worker.name}] Instance successfully signaled to stop.")
                return True
            except ClientError as e:
                logger.error(f"[{worker.name}] Failed to stop instance (attempt {attempt}/{attempts}): {e}")
                if attempt < attempts:
                    time.sleep(2 ** attempt)
        return False

    def wait_for_ssm_ready(self, worker: WorkerNode, timeout_sec: int = 180) -> bool:
        """Polls AWS Systems Manager until instance reports Online status."""
        ssm = self._get_ssm(worker.region)
        start_time = time.time()
        logger.info(f"[{worker.name}] Waiting for SSM agent to report Online (timeout: {timeout_sec}s)...")
        delay = 2.0

        while time.time() - start_time < timeout_sec:
            try:
                resp = ssm.describe_instance_information(
                    Filters=[{"Key": "InstanceIds", "Values": [worker.instance_id]}]
                )
                instances = resp.get("InstanceInformationList", [])
                if instances and instances[0].get("PingStatus") == "Online":
                    logger.info(f"[{worker.name}] SSM Agent Online and ready for remote execution.")
                    return True
            except ClientError as e:
                logger.debug(f"[{worker.name}] SSM poll error: {e}")

            time.sleep(delay)
            delay = min(delay * 2, 15.0)  # exponential backoff, capped

        logger.error(f"[{worker.name}] Timed out waiting for SSM agent after {timeout_sec}s.")
        return False
