import pytest
from pathlib import Path
from src.config import FleetConfig, WorkerNode, StorageConfig, ExecutionPolicy

def test_config_parsing(tmp_path):
    yaml_content = """
fleet_name: "Test-Fleet"
workers:
  - name: "Worker-01"
    region: "eu-north-1"
    instance_id: "i-1234567890abcdef0"
    instance_type: "c6i.xlarge"
    auto_stop: true
storage:
  bucket_name: "test-bucket"
execution:
  max_parallel_workers: 3
  command_timeout_sec: 300
"""
    cfg_file = tmp_path / "fleet.yaml"
    cfg_file.write_text(yaml_content, encoding="utf-8")

    cfg = FleetConfig.from_yaml(cfg_file)
    assert cfg.fleet_name == "Test-Fleet"
    assert len(cfg.workers) == 1
    assert cfg.workers[0].region == "eu-north-1"
    assert cfg.workers[0].auto_stop is True
    assert cfg.storage.bucket_name == "test-bucket"
    assert cfg.execution.max_parallel_workers == 3
