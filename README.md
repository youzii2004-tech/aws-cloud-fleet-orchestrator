# AWS Cloud Fleet Orchestrator

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![AWS Boto3](https://img.shields.io/badge/aws-boto3-orange.svg)](https://aws.amazon.com/sdk-for-python/)
[![License: MIT](https://img.shields.io/badge/License-MIT-emerald.svg)](LICENSE)
[![Tests: Pytest](https://img.shields.io/badge/tests-pytest-green.svg)](tests/)

**Distributed Multi-Region Compute Fleet Orchestrator** managing ephemeral EC2 worker instances across global AWS availability zones. Features **Zero-Idle Power Lifecycle Management**, remote execution via **AWS Systems Manager (SSM) RunCommand**, concurrent batch dispatching, and automated S3 artifact ingestion.

---

## Architecture Overview

```
                      ┌────────────────────────────────────────┐
                      │        Fleet Task Dispatcher           │
                      │       (ThreadPoolExecutor)             │
                      └──────────────────┬─────────────────────┘
                                         │
        ┌────────────────────────────────┼────────────────────────────────┐
        │                                │                                │
        ▼                                ▼                                ▼
┌─────────────────┐            ┌─────────────────┐            ┌─────────────────┐
│ Worker 01       │            │ Worker 02       │            │ Worker 03, 04.. │
│ Region:         │            │ Region:         │            │ Regions:        │
│ eu-north-1      │            │ eu-central-1    │            │ eu-west-1, US   │
│ (Stockholm)     │            │ (Frankfurt)     │            │                 │
├─────────────────┤            ├─────────────────┤            ├─────────────────┤
│ • Auto-Power On │            │ • Auto-Power On │            │ • Auto-Power On │
│ • SSM Execution │            │ • SSM Execution │            │ • SSM Execution │
│ • S3 Sync       │            │ • S3 Sync       │            │ • S3 Sync       │
│ • Auto-Stop ($0)│            │ • Auto-Stop ($0)│            │ • Auto-Stop ($0)│
└────────┬────────┘            └────────┬────────┘            └────────┬────────┘
         │                              │                              │
         └──────────────────────────────┼──────────────────────────────┘
                                        ▼
                      ┌────────────────────────────────────────┐
                      │         S3 Artifact Lake Hub           │
                      │     (Centralized Storage Sync)         │
                      └────────────────────────────────────────┘
```

---

## Key Engineering Pillars

### 1. Zero-Idle Power Lifecycle Policy
Cloud compute waste occurs when worker instances remain in `running` state while idling. The orchestrator enforces an on-demand power state machine:
- Instances boot only when tasks enter the execution queue.
- Systems Manager ping status is verified (`PingStatus == "Online"`).
- Upon task termination (success or failure), instances are transitioned back to `stopped` state. When several tasks share one worker, it is reference-counted and stopped only after the last task finishes.
- Stop calls are retried; if they still fail, the result carries `stop_failed: true` and a CRITICAL log is emitted, since the instance may still be billing.
- **Result:** no idle *compute* charges. Stopped instances still incur EBS volume (and any Elastic IP) costs.

### 2. Portless Remote Execution via AWS Systems Manager (SSM)
Eliminates traditional SSH access, bastion hosts, and open inbound ports (port 22):
- Commands are dispatched securely over the AWS control plane using `AWS-RunShellScript`.
- Polling with capped exponential backoff (1s up to 15s) tracks invocation lifecycle (`Pending`, `Executing`, `Success`, `Failed`).
- Full stdout and stderr capture with structured execution telemetry.

### 3. Multi-Region Parallel Dispatch
- Uses `concurrent.futures.ThreadPoolExecutor` to execute tasks across distinct AWS regions in parallel.
- Distributes computational workloads across geographically isolated availability zones (e.g., Stockholm, Frankfurt, Dublin, Ohio, Oregon) to mitigate regional quota exhaustion.

---

## Directory Structure

```text
aws-cloud-fleet-orchestrator/
├── config/
│   └── fleet_config.example.yaml   # Multi-region worker topologies
├── dashboard/
│   └── index.html                  # Interactive Cloud Fleet Mission Control Monitor
├── scripts/
│   └── setup_worker.sh             # EC2 user-data provisioning bootstrap
├── src/
│   ├── config.py                   # Pydantic schema validation
│   ├── fleet_manager.py            # EC2 power lifecycle engine
│   ├── ssm_executor.py             # SSM RunCommand dispatch & polling
│   ├── task_dispatcher.py          # Parallel multi-region batch runner
│   └── s3_sync.py                  # S3 asset ingestion & streaming
├── tests/
│   ├── test_config.py              # Configuration schema tests
│   └── test_ssm_executor.py        # Mocked SSM execution tests
├── LICENSE                         # MIT License
├── README.md
└── requirements.txt
```

---

## Quickstart

### 1. Installation
```bash
git clone https://github.com/youzii2004-tech/aws-cloud-fleet-orchestrator.git
cd aws-cloud-fleet-orchestrator
pip install -r requirements.txt
```

### 2. Configure Workers
Copy the example configuration and specify your AWS instance IDs and regions:
```bash
cp config/fleet_config.example.yaml config/fleet_config.yaml
```

### 3. Dispatch Parallel Tasks
```python
from pathlib import Path
from src.config import FleetConfig
from src.task_dispatcher import TaskDispatcher, FleetTask

# Load configuration
config = FleetConfig.from_yaml(Path("config/fleet_config.yaml"))
dispatcher = TaskDispatcher(config=config, aws_profile="default")

# Define batch tasks
tasks = [
    FleetTask(task_id="TASK-01", commands=["python3 -c 'print(\"Task on Worker 1\")'"], target_worker_name="Worker-01-Stockholm"),
    FleetTask(task_id="TASK-02", commands=["python3 -c 'print(\"Task on Worker 2\")'"], target_worker_name="Worker-02-Frankfurt"),
]

# Dispatch concurrently across regions
results = dispatcher.dispatch_batch(tasks)
for res in results:
    print(f"[{res['worker']}] Status: {res['status']} ({res['elapsed_sec']:.1f}s)")
```

### 4. Run Unit Tests
```bash
pytest tests/
```

---

## Security Model
- **Zero Inbound Security Groups:** Instances require no public IP addresses or open inbound ports.
- **IAM Least Privilege:** Relies strictly on `AmazonSSMManagedInstanceCore` and targeted S3 read/write policies.
- **Encrypted Control Plane:** All remote commands and artifact transfers travel over TLS-encrypted AWS API endpoints.

---

## License
MIT License. Created by Muhammad Usaid Siddiqui.
