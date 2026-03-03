"""Thread-safe in-memory state store with pre-configured defaults."""

import threading
from typing import Dict, List, Optional

ACCOUNT_ID = "123456789012"
REGION = "us-east-1"
ARN_PREFIX = f"arn:aws:batch:{REGION}:{ACCOUNT_ID}"

DEFAULT_QUEUE = "corral-default"
DEFAULT_COMPUTE_ENV = "corral-default"


def _ce_arn(name: str) -> str:
    return f"{ARN_PREFIX}:compute-environment/{name}"


def _queue_arn(name: str) -> str:
    return f"{ARN_PREFIX}:job-queue/{name}"


def _jobdef_arn(name: str, revision: int) -> str:
    return f"{ARN_PREFIX}:job-definition/{name}:{revision}"


def _job_arn(job_id: str) -> str:
    return f"{ARN_PREFIX}:job/{job_id}"


class Store:
    def __init__(self, queue_name: str = DEFAULT_QUEUE):
        self._lock = threading.Lock()
        self._job_definitions: Dict[str, dict] = {}  # arn -> definition
        self._jobs: Dict[str, dict] = {}  # job_id -> job
        self._queues: Dict[str, dict] = {}  # name -> queue
        self._compute_envs: Dict[str, dict] = {}  # name -> env
        self._init_defaults(queue_name)

    def _init_defaults(self, queue_name: str):
        ce = {
            "computeEnvironmentName": DEFAULT_COMPUTE_ENV,
            "computeEnvironmentArn": _ce_arn(DEFAULT_COMPUTE_ENV),
            "type": "MANAGED",
            "state": "ENABLED",
            "status": "VALID",
            "statusReason": "ComputeEnvironment Healthy",
            "computeResources": {
                "type": "EC2",
                "minvCpus": 0,
                "maxvCpus": 256,
                "instanceTypes": ["optimal"],
            },
        }
        self._compute_envs[DEFAULT_COMPUTE_ENV] = ce

        queue = {
            "jobQueueName": queue_name,
            "jobQueueArn": _queue_arn(queue_name),
            "state": "ENABLED",
            "status": "VALID",
            "statusReason": "JobQueue Healthy",
            "priority": 1,
            "computeEnvironmentOrder": [
                {"order": 1, "computeEnvironment": ce["computeEnvironmentArn"]}
            ],
        }
        self._queues[queue_name] = queue

    # --- Job definitions ---

    def put_job_definition(self, arn: str, definition: dict):
        with self._lock:
            self._job_definitions[arn] = definition

    def get_job_definitions(
        self,
        name: Optional[str] = None,
        arns: Optional[List[str]] = None,
        status: Optional[str] = None,
    ) -> List[dict]:
        with self._lock:
            defs = list(self._job_definitions.values())
        if name:
            defs = [d for d in defs if d["jobDefinitionName"] == name]
        if arns:
            arn_set = set(arns)
            defs = [
                d
                for d in defs
                if d["jobDefinitionArn"] in arn_set
                or d["jobDefinitionName"] in arn_set
            ]
        if status:
            defs = [d for d in defs if d.get("status") == status]
        return defs

    def get_job_definition_by_arn(self, arn: str) -> Optional[dict]:
        with self._lock:
            return self._job_definitions.get(arn)

    def next_revision(self, name: str) -> int:
        with self._lock:
            revisions = [
                d["revision"]
                for d in self._job_definitions.values()
                if d["jobDefinitionName"] == name
            ]
        return max(revisions, default=0) + 1

    # --- Jobs ---

    def put_job(self, job_id: str, job: dict):
        with self._lock:
            self._jobs[job_id] = job

    def update_job(self, job_id: str, **kwargs):
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id].update(kwargs)

    def get_jobs(
        self,
        job_ids: Optional[List[str]] = None,
        queue: Optional[str] = None,
        status: Optional[str] = None,
    ) -> List[dict]:
        with self._lock:
            jobs = list(self._jobs.values())
        if job_ids:
            id_set = set(job_ids)
            jobs = [j for j in jobs if j["jobId"] in id_set]
        if queue:
            jobs = [
                j
                for j in jobs
                if j.get("jobQueue") == queue
                or j.get("jobQueue", "").endswith(f"/{queue}")
            ]
        if status:
            jobs = [j for j in jobs if j.get("status") == status]
        return jobs

    # --- Queues & compute envs ---

    def get_queues(self, names: Optional[List[str]] = None) -> List[dict]:
        with self._lock:
            queues = list(self._queues.values())
        if names:
            name_set = set(names)
            queues = [
                q
                for q in queues
                if q["jobQueueName"] in name_set or q["jobQueueArn"] in name_set
            ]
        return queues

    def get_compute_envs(self, names: Optional[List[str]] = None) -> List[dict]:
        with self._lock:
            envs = list(self._compute_envs.values())
        if names:
            name_set = set(names)
            envs = [
                e
                for e in envs
                if e["computeEnvironmentName"] in name_set
                or e["computeEnvironmentArn"] in name_set
            ]
        return envs
