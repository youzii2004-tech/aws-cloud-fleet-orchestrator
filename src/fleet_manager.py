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

    def start_worker(self, worker: WorkerNode, wait_for_online: bool = True) -> bool:
        """Starts an EC2 instance and optionally awaits SSM agent readiness."""
        ec2 = self._get_ec2(worker.region)
        logger.info(f"[{worker.name}] Starting instance {worker.instance_id} in {worker.region}...")
        try:
            ec2.start_instances(InstanceIds=[worker.instance_id])
        except ClientError as e:
            if "IncorrectInstanceState" in str(e):
                logger.warning(f"[{worker.name}] Instance already running or starting.")
            else:
                logger.error(f"[{worker.name}] Failed to start instance: {e}")
                raise

        if wait_for_online:
            return self.wait_for_ssm_ready(worker)
        return True

    def stop_worker(self, worker: WorkerNode) -> bool:
        """Stops an EC2 instance to halt all compute charges ($0 idle compute cost)."""
        ec2 = self._get_ec2(worker.region)
        logger.info(f"[{worker.name}] Stopping instance {worker.instance_id} (Zero-Idle Cost Enforced)...")
        try:
            ec2.stop_instances(InstanceIds=[worker.instance_id])
            logger.info(f"[{worker.name}] Instance successfully signaled to stop.")
            return True
        except ClientError as e:
            logger.error(f"[{worker.name}] Failed to stop instance: {e}")
            return False

    def wait_for_ssm_ready(self, worker: WorkerNode, timeout_sec: int = 180) -> bool:
        """Polls AWS Systems Manager until instance reports Online status."""
        ssm = self._get_ssm(worker.region)
        start_time = time.time()
        logger.info(f"[{worker.name}] Waiting for SSM agent to report Online (timeout: {timeout_sec}s)...")

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

            time.sleep(5)

        logger.error(f"[{worker.name}] Timed out waiting for SSM agent after {timeout_sec}s.")
        return False
