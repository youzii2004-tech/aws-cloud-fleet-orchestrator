"""
task_dispatcher.py — Concurrent multi-region batch task dispatcher.
Orchestrates parallel task execution across distributed EC2 instances using ThreadPoolExecutor,
enforcing pre-flight health checks, SSM command execution, artifact collection, and automatic power teardown.
"""
import concurrent.futures
import logging
import time
from typing import List, Dict, Any, Optional
from .config import FleetConfig, WorkerNode
from .fleet_manager import FleetManager
from .ssm_executor import SSMExecutor, ExecutionResult
from .s3_sync import S3SyncManager

logger = logging.getLogger("TaskDispatcher")


class FleetTask:
    def __init__(self, task_id: str, commands: List[str], target_worker_name: Optional[str] = None):
        self.task_id = task_id
        self.commands = commands
        self.target_worker_name = target_worker_name


class TaskDispatcher:
    def __init__(self, config: FleetConfig, aws_profile: Optional[str] = None):
        self.config = config
        self.fleet_manager = FleetManager(aws_profile=aws_profile)
        self.ssm_executor = SSMExecutor(aws_profile=aws_profile)
        self.s3_sync = S3SyncManager(
            bucket_name=config.storage.bucket_name,
            aws_profile=aws_profile
        )

    def _execute_worker_pipeline(self, worker: WorkerNode, task: FleetTask) -> Dict[str, Any]:
        """Lifecycle workflow for a single worker node."""
        worker_name = worker.name
        start_ts = time.time()
        result_payload = {
            "task_id": task.task_id,
            "worker": worker_name,
            "region": worker.region,
            "instance_id": worker.instance_id,
            "status": "Failed",
            "execution_result": None,
            "elapsed_sec": 0.0
        }

        try:
            # 1. Power On & Verify SSM Readiness
            online = self.fleet_manager.start_worker(worker, wait_for_online=True)
            if not online:
                logger.error(f"[{worker_name}] Worker failed to report online. Aborting task {task.task_id}.")
                return result_payload

            # 2. Dispatch Task via SSM RunCommand
            exec_res = self.ssm_executor.run_command(
                worker=worker,
                commands=task.commands,
                timeout_sec=self.config.execution.command_timeout_sec,
                comment=f"Task {task.task_id}"
            )
            result_payload["execution_result"] = exec_res
            result_payload["status"] = "Success" if exec_res.is_success else "ExecutionFailed"

        except Exception as e:
            logger.error(f"[{worker_name}] Unhandled exception in task {task.task_id}: {e}")
            result_payload["status"] = "Error"
        finally:
            # 3. Enforce Zero-Idle Cost Policy: Stop instance unless auto_stop=False
            if worker.auto_stop:
                logger.info(f"[{worker_name}] Enforcing Zero-Idle policy. Powering down worker...")
                self.fleet_manager.stop_worker(worker)
            result_payload["elapsed_sec"] = time.time() - start_ts

        return result_payload

    def dispatch_batch(self, tasks: List[FleetTask]) -> List[Dict[str, Any]]:
        """Dispatches an array of tasks across available fleet workers concurrently."""
        workers = self.config.workers
        if not workers:
            raise ValueError("Fleet has no configured worker nodes.")

        logger.info(f"Dispatching batch of {len(tasks)} tasks across {len(workers)} fleet workers...")
        results = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.execution.max_parallel_workers) as executor:
            future_to_task = {}
            for i, task in enumerate(tasks):
                # Bind to specific worker if requested, otherwise round-robin
                worker = next((w for w in workers if w.name == task.target_worker_name), None) if task.target_worker_name else workers[i % len(workers)]
                fut = executor.submit(self._execute_worker_pipeline, worker, task)
                future_to_task[fut] = task

            for fut in concurrent.futures.as_completed(future_to_task):
                task = future_to_task[fut]
                try:
                    res = fut.result()
                    results.append(res)
                    logger.info(f"Task {task.task_id} on {res['worker']} finished with status: {res['status']}.")
                except Exception as e:
                    logger.error(f"Task {task.task_id} generated unexpected worker exception: {e}")
                    results.append({"task_id": task.task_id, "status": "Failed", "error": str(e)})

        return results
