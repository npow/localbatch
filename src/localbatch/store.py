"""Thread-safe in-memory state store with pre-configured defaults."""

import threading
from typing import Dict, List, Optional

ACCOUNT_ID = "123456789012"
REGION = "us-east-1"
ARN_PREFIX = f"arn:aws:batch:{REGION}:{ACCOUNT_ID}"

DEFAULT_QUEUE = "localbatch-default"
DEFAULT_COMPUTE_ENV = "localbatch-default"


def _ce_arn(name: str) -> str:
    return f"{ARN_PREFIX}:compute-environment/{name}"


def _queue_arn(name: str) -> str:
    return f"{ARN_PREFIX}:job-queue/{name}"


def _jobdef_arn(name: str, revision: int) -> str:
    return f"{ARN_PREFIX}:job-definition/{name}:{revision}"


def _job_arn(job_id: str) -> str:
    return f"{ARN_PREFIX}:job/{job_id}"


def _sp_arn(name: str) -> str:
    return f"{ARN_PREFIX}:scheduling-policy/{name}"


class Store:
    def __init__(self, queue_name: str = DEFAULT_QUEUE):
        self._lock = threading.Lock()
        self._job_definitions: Dict[str, dict] = {}  # arn -> definition
        self._jobs: Dict[str, dict] = {}  # job_id -> job
        self._queues: Dict[str, dict] = {}  # name -> queue
        self._compute_envs: Dict[str, dict] = {}  # name -> env
        self._scheduling_policies: Dict[str, dict] = {}  # arn -> policy
        self._tags: Dict[str, dict] = {}  # resource_arn -> {key: value}
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
            "tags": {},
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
            "tags": {},
        }
        self._queues[queue_name] = queue

    # ------------------------------------------------------------------
    # Job definitions
    # ------------------------------------------------------------------

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

    def deregister_job_definition(self, ref: str) -> bool:
        """Set a job definition's status to INACTIVE. ref is name:revision or ARN."""
        with self._lock:
            if ref in self._job_definitions:
                self._job_definitions[ref]["status"] = "INACTIVE"
                return True
            # name:revision form
            for arn, d in self._job_definitions.items():
                name_rev = f"{d['jobDefinitionName']}:{d['revision']}"
                if ref == name_rev or ref == d["jobDefinitionName"]:
                    d["status"] = "INACTIVE"
                    return True
        return False

    # ------------------------------------------------------------------
    # Jobs
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Queues
    # ------------------------------------------------------------------

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

    def create_queue(self, name: str, body: dict) -> dict:
        arn = _queue_arn(name)
        queue = {
            "jobQueueName": name,
            "jobQueueArn": arn,
            "state": body.get("state", "ENABLED"),
            "status": "VALID",
            "statusReason": "JobQueue Healthy",
            "priority": body.get("priority", 1),
            "computeEnvironmentOrder": body.get("computeEnvironmentOrder", []),
            "schedulingPolicyArn": body.get("schedulingPolicyArn"),
            "tags": body.get("tags", {}),
        }
        with self._lock:
            self._queues[name] = queue
        return queue

    def update_queue(self, ref: str, body: dict) -> Optional[dict]:
        with self._lock:
            queue = self._queues.get(ref) or next(
                (q for q in self._queues.values() if q["jobQueueArn"] == ref), None
            )
            if queue is None:
                return None
            for field in ("state", "priority", "computeEnvironmentOrder", "schedulingPolicyArn"):
                if field in body:
                    queue[field] = body[field]
            return queue

    def delete_queue(self, ref: str) -> bool:
        with self._lock:
            name = ref if ref in self._queues else next(
                (n for n, q in self._queues.items() if q["jobQueueArn"] == ref), None
            )
            if name is None:
                return False
            del self._queues[name]
            return True

    # ------------------------------------------------------------------
    # Compute environments
    # ------------------------------------------------------------------

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

    def create_compute_env(self, name: str, body: dict) -> dict:
        arn = _ce_arn(name)
        ce = {
            "computeEnvironmentName": name,
            "computeEnvironmentArn": arn,
            "type": body.get("type", "MANAGED"),
            "state": body.get("state", "ENABLED"),
            "status": "VALID",
            "statusReason": "ComputeEnvironment Healthy",
            "computeResources": body.get("computeResources", {}),
            "serviceRole": body.get("serviceRole"),
            "tags": body.get("tags", {}),
        }
        with self._lock:
            self._compute_envs[name] = ce
        return ce

    def update_compute_env(self, ref: str, body: dict) -> Optional[dict]:
        with self._lock:
            ce = self._compute_envs.get(ref) or next(
                (e for e in self._compute_envs.values() if e["computeEnvironmentArn"] == ref),
                None,
            )
            if ce is None:
                return None
            for field in ("state", "computeResources", "serviceRole"):
                if field in body:
                    ce[field] = body[field]
            return ce

    def delete_compute_env(self, ref: str) -> bool:
        with self._lock:
            name = ref if ref in self._compute_envs else next(
                (n for n, e in self._compute_envs.items() if e["computeEnvironmentArn"] == ref),
                None,
            )
            if name is None:
                return False
            del self._compute_envs[name]
            return True

    # ------------------------------------------------------------------
    # Scheduling policies
    # ------------------------------------------------------------------

    def create_scheduling_policy(self, name: str, body: dict) -> dict:
        arn = _sp_arn(name)
        policy = {
            "arn": arn,
            "name": name,
            "fairsharePolicy": body.get("fairsharePolicy", {}),
            "tags": body.get("tags", {}),
        }
        with self._lock:
            self._scheduling_policies[arn] = policy
        return policy

    def get_scheduling_policies(self, arns: Optional[List[str]] = None) -> List[dict]:
        with self._lock:
            policies = list(self._scheduling_policies.values())
        if arns:
            arn_set = set(arns)
            policies = [p for p in policies if p["arn"] in arn_set]
        return policies

    def update_scheduling_policy(self, arn: str, body: dict) -> Optional[dict]:
        with self._lock:
            policy = self._scheduling_policies.get(arn)
            if policy is None:
                return None
            if "fairsharePolicy" in body:
                policy["fairsharePolicy"] = body["fairsharePolicy"]
            return policy

    def delete_scheduling_policy(self, arn: str) -> bool:
        with self._lock:
            if arn not in self._scheduling_policies:
                return False
            del self._scheduling_policies[arn]
            return True

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------

    def put_tags(self, resource_arn: str, tags: dict):
        with self._lock:
            existing = self._tags.setdefault(resource_arn, {})
            existing.update(tags)

    def get_tags(self, resource_arn: str) -> dict:
        with self._lock:
            return dict(self._tags.get(resource_arn, {}))

    def delete_tags(self, resource_arn: str, keys: List[str]):
        with self._lock:
            existing = self._tags.get(resource_arn, {})
            for k in keys:
                existing.pop(k, None)
