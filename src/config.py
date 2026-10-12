"""
config.py — Fleet configuration models and parser.
Defines schema for multi-region EC2 worker pools, SSM execution policies, and S3 sync endpoints.
"""
from typing import List, Dict, Optional
from pydantic import BaseModel, Field
import yaml
from pathlib import Path


class WorkerNode(BaseModel):
    name: str = Field(..., description="Logical name of worker node (e.g. Worker-01)")
    region: str = Field(..., description="AWS Region identifier (e.g. eu-north-1)")
    instance_id: str = Field(..., description="EC2 Instance ID (e.g. i-0f0d7f21cf4f7fc50)")
    instance_type: str = Field(default="c6i.xlarge", description="EC2 instance type")
    profile_id: Optional[str] = Field(default=None, description="Worker identity partition")
    auto_stop: bool = Field(default=True, description="Stop instance immediately after job completes to eliminate idle cost")


class StorageConfig(BaseModel):
    bucket_name: str = Field(..., description="S3 bucket for task payloads and artifact ingestion")
    artifact_prefix: str = Field(default="artifacts/", description="Prefix for output artifacts")
    reference_prefix: str = Field(default="references/", description="Prefix for input references")


class ExecutionPolicy(BaseModel):
    max_parallel_workers: int = Field(default=5, description="Maximum concurrent worker threads")
    command_timeout_sec: int = Field(default=900, description="SSM RunCommand execution timeout")
    poll_interval_sec: float = Field(default=2.5, description="Interval between SSM status checks")
    retry_attempts: int = Field(default=2, description="Number of execution retries upon non-zero exit")


class FleetConfig(BaseModel):
    fleet_name: str = Field(default="Distributed-Cloud-Fleet", description="Identifier for this fleet deployment")
    workers: List[WorkerNode] = Field(default_factory=list, description="Registered worker instances")
    storage: StorageConfig
    execution: ExecutionPolicy = Field(default_factory=ExecutionPolicy)

    @classmethod
    def from_yaml(cls, path: Path) -> "FleetConfig":
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return cls(**data)
