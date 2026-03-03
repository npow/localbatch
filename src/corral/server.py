"""FastAPI application implementing the full AWS Batch REST API surface."""

import time
import uuid
from typing import List, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

ACCOUNT_ID = "123456789012"
REGION = "us-east-1"


def create_app(store, runner) -> FastAPI:
    app = FastAPI(title="corral", description="Local AWS Batch emulator")

    # ------------------------------------------------------------------
    # Job Definitions
    # ------------------------------------------------------------------

    @app.post("/v1/registerjobdefinition")
    async def register_job_definition(request: Request):
        body = await request.json()
        name = body["jobDefinitionName"]
        revision = store.next_revision(name)
        arn = f"arn:aws:batch:{REGION}:{ACCOUNT_ID}:job-definition/{name}:{revision}"
        store.put_job_definition(
            arn,
            {
                **body,
                "jobDefinitionArn": arn,
                "revision": revision,
                "status": "ACTIVE",
                "tags": body.get("tags", {}),
            },
        )
        return {"jobDefinitionArn": arn, "jobDefinitionName": name, "revision": revision}

    @app.post("/v1/describejobdefinitions")
    async def describe_job_definitions(request: Request):
        body = await request.json()
        defs = store.get_job_definitions(
            name=body.get("jobDefinitionName"),
            arns=body.get("jobDefinitions"),
            status=body.get("status"),
        )
        return {"jobDefinitions": defs, "nextToken": None}

    @app.post("/v1/deregisterjobdefinition")
    async def deregister_job_definition(request: Request):
        body = await request.json()
        ref = body["jobDefinition"]
        if not store.deregister_job_definition(ref):
            return JSONResponse(
                status_code=400,
                content={"message": f"Job definition not found: {ref}"},
            )
        return {}

    # ------------------------------------------------------------------
    # Job Submission & Control
    # ------------------------------------------------------------------

    @app.post("/v1/submitjob")
    async def submit_job(request: Request):
        body = await request.json()
        job_id = str(uuid.uuid4())
        job_def_arn = _resolve_job_def_arn(store, body["jobDefinition"])
        if job_def_arn is None:
            return JSONResponse(
                status_code=400,
                content={"message": f"Job definition not found: {body['jobDefinition']}"},
            )

        job = {
            "jobId": job_id,
            "jobArn": f"arn:aws:batch:{REGION}:{ACCOUNT_ID}:job/{job_id}",
            "jobName": body["jobName"],
            "jobQueue": body["jobQueue"],
            "jobDefinition": job_def_arn,
            "status": "SUBMITTED",
            "statusReason": "Job submitted",
            "createdAt": _now_ms(),
            "startedAt": None,
            "stoppedAt": None,
            "containerOverrides": body.get("containerOverrides", {}),
            "timeout": body.get("timeout", {}),
            "retryStrategy": body.get("retryStrategy", {"attempts": 1}),
            "tags": body.get("tags", {}),
            "parameters": body.get("parameters", {}),
        }
        store.put_job(job_id, job)
        runner.submit(job)
        return {"jobId": job_id, "jobName": body["jobName"], "jobArn": job["jobArn"]}

    @app.post("/v1/describejobs")
    async def describe_jobs(request: Request):
        body = await request.json()
        jobs = store.get_jobs(job_ids=body.get("jobs", []))
        return {"jobs": [_format_job(j) for j in jobs]}

    @app.post("/v1/canceljob")
    async def cancel_job(request: Request):
        """Cancel a job that has not yet started (SUBMITTED/PENDING/RUNNABLE)."""
        body = await request.json()
        runner.terminate(body["jobId"], body.get("reason", "Cancelled by user"))
        return {}

    @app.post("/v1/terminatejob")
    async def terminate_job(request: Request):
        body = await request.json()
        runner.terminate(body["jobId"], body.get("reason", "Terminated by user"))
        return {}

    # ------------------------------------------------------------------
    # Job Listing
    # ------------------------------------------------------------------

    @app.post("/v1/listjobs")
    async def list_jobs(request: Request):
        body = await request.json()
        jobs = store.get_jobs(queue=body.get("jobQueue"), status=body.get("jobStatus"))
        summaries = [
            {
                "jobId": j["jobId"],
                "jobName": j["jobName"],
                "status": j["status"],
                "createdAt": j.get("createdAt"),
                "startedAt": j.get("startedAt"),
                "stoppedAt": j.get("stoppedAt"),
            }
            for j in jobs
        ]
        return {"jobSummaryList": summaries, "nextToken": None}

    @app.post("/v1/getjobqueuesnapshot")
    async def get_job_queue_snapshot(request: Request):
        body = await request.json()
        queue_ref = body.get("jobQueue", "")
        # All non-terminal jobs in priority order
        active_statuses = {"SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"}
        jobs = store.get_jobs(queue=queue_ref)
        front = [
            {
                "id": j["jobId"],
                "priority": 1,
                "earliestGetResultsAt": j.get("createdAt", _now_ms()),
            }
            for j in jobs
            if j.get("status") in active_statuses
        ]
        return {
            "frontOfQueue": {
                "jobs": front,
                "lastUpdatedAt": _now_ms(),
            }
        }

    # ------------------------------------------------------------------
    # Job Queues
    # ------------------------------------------------------------------

    @app.post("/v1/describejobqueues")
    async def describe_job_queues(request: Request):
        body = await request.json()
        return {
            "jobQueues": store.get_queues(names=body.get("jobQueues")),
            "nextToken": None,
        }

    @app.post("/v1/createjobqueue")
    async def create_job_queue(request: Request):
        body = await request.json()
        name = body["jobQueueName"]
        queue = store.create_queue(name, body)
        return {"jobQueueArn": queue["jobQueueArn"], "jobQueueName": name}

    @app.post("/v1/updatejobqueue")
    async def update_job_queue(request: Request):
        body = await request.json()
        ref = body["jobQueue"]
        queue = store.update_queue(ref, body)
        if queue is None:
            return JSONResponse(
                status_code=400,
                content={"message": f"Job queue not found: {ref}"},
            )
        return {"jobQueueArn": queue["jobQueueArn"], "jobQueueName": queue["jobQueueName"]}

    @app.post("/v1/deletejobqueue")
    async def delete_job_queue(request: Request):
        body = await request.json()
        store.delete_queue(body["jobQueue"])
        return {}

    # ------------------------------------------------------------------
    # Compute Environments
    # ------------------------------------------------------------------

    @app.post("/v1/describecomputeenvironments")
    async def describe_compute_environments(request: Request):
        body = await request.json()
        return {
            "computeEnvironments": store.get_compute_envs(
                names=body.get("computeEnvironments")
            ),
            "nextToken": None,
        }

    @app.post("/v1/createcomputeenvironment")
    async def create_compute_environment(request: Request):
        body = await request.json()
        name = body["computeEnvironmentName"]
        ce = store.create_compute_env(name, body)
        return {
            "computeEnvironmentArn": ce["computeEnvironmentArn"],
            "computeEnvironmentName": name,
        }

    @app.post("/v1/updatecomputeenvironment")
    async def update_compute_environment(request: Request):
        body = await request.json()
        ref = body["computeEnvironment"]
        ce = store.update_compute_env(ref, body)
        if ce is None:
            return JSONResponse(
                status_code=400,
                content={"message": f"Compute environment not found: {ref}"},
            )
        return {
            "computeEnvironmentArn": ce["computeEnvironmentArn"],
            "computeEnvironmentName": ce["computeEnvironmentName"],
        }

    @app.post("/v1/deletecomputeenvironment")
    async def delete_compute_environment(request: Request):
        body = await request.json()
        store.delete_compute_env(body["computeEnvironment"])
        return {}

    # ------------------------------------------------------------------
    # Scheduling Policies
    # ------------------------------------------------------------------

    @app.post("/v1/createschedulingpolicy")
    async def create_scheduling_policy(request: Request):
        body = await request.json()
        name = body["name"]
        policy = store.create_scheduling_policy(name, body)
        return {"arn": policy["arn"], "name": name}

    @app.post("/v1/describeschedulingpolicies")
    async def describe_scheduling_policies(request: Request):
        body = await request.json()
        policies = store.get_scheduling_policies(arns=body.get("arns"))
        return {"schedulingPolicies": policies}

    @app.post("/v1/listschedulingpolicies")
    async def list_scheduling_policies(request: Request):
        policies = store.get_scheduling_policies()
        return {
            "schedulingPolicies": [{"arn": p["arn"], "name": p["name"]} for p in policies],
            "nextToken": None,
        }

    @app.post("/v1/updateschedulingpolicy")
    async def update_scheduling_policy(request: Request):
        body = await request.json()
        arn = body["arn"]
        policy = store.update_scheduling_policy(arn, body)
        if policy is None:
            return JSONResponse(
                status_code=400,
                content={"message": f"Scheduling policy not found: {arn}"},
            )
        return {}

    @app.post("/v1/deleteschedulingpolicy")
    async def delete_scheduling_policy(request: Request):
        body = await request.json()
        store.delete_scheduling_policy(body["arn"])
        return {}

    # ------------------------------------------------------------------
    # Tags
    # ------------------------------------------------------------------

    @app.get("/v1/tags/{resource_arn:path}")
    async def list_tags_for_resource(resource_arn: str):
        return {"tags": store.get_tags(resource_arn)}

    @app.post("/v1/tags/{resource_arn:path}")
    async def tag_resource(resource_arn: str, request: Request):
        body = await request.json()
        store.put_tags(resource_arn, body.get("tags", {}))
        return {}

    @app.delete("/v1/tags/{resource_arn:path}")
    async def untag_resource(resource_arn: str, request: Request):
        tag_keys: List[str] = request.query_params.getlist("tagKeys")
        store.delete_tags(resource_arn, tag_keys)
        return {}

    # ------------------------------------------------------------------
    # Fake ECS Container Metadata endpoint
    #
    # Injected into containers as ECS_CONTAINER_METADATA_URI_V4 so
    # Metaflow's batch_decorator can read a synthetic CloudWatch log stream
    # name without hitting real AWS.
    # ------------------------------------------------------------------

    @app.get("/metadata/{job_id}/task")
    async def ecs_metadata_task(job_id: str):
        return {
            "TaskARN": f"arn:aws:ecs:{REGION}:{ACCOUNT_ID}:task/corral/{job_id}",
            "Family": "corral-task",
            "Revision": "1",
            "Containers": [
                {
                    "DockerId": job_id,
                    "Name": "corral-container",
                    "LogDriver": "awslogs",
                    "LogOptions": {
                        "awslogs-group": "/corral/batch/job",
                        "awslogs-region": REGION,
                        "awslogs-stream": f"corral/default/{job_id}",
                    },
                }
            ],
        }

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _resolve_job_def_arn(store, ref: str) -> Optional[str]:
    """Resolve a job definition name[:revision] or ARN to a full ARN."""
    if ref.startswith("arn:"):
        return ref if store.get_job_definition_by_arn(ref) else None

    # name or name:revision
    parts = ref.rsplit(":", 1)
    name = parts[0]
    revision = int(parts[1]) if len(parts) == 2 else None

    defs = store.get_job_definitions(name=name, status="ACTIVE")
    if not defs:
        return None
    if revision is not None:
        defs = [d for d in defs if d["revision"] == revision]
    if not defs:
        return None
    return sorted(defs, key=lambda d: d["revision"])[-1]["jobDefinitionArn"]


def _format_job(job: dict) -> dict:
    """Shape a stored job dict into the AWS Batch DescribeJobs response format."""
    return {
        "jobId": job["jobId"],
        "jobArn": job.get("jobArn", ""),
        "jobName": job["jobName"],
        "jobQueue": job["jobQueue"],
        "jobDefinition": job["jobDefinition"],
        "status": job["status"],
        "statusReason": job.get("statusReason", ""),
        "createdAt": job.get("createdAt"),
        "startedAt": job.get("startedAt"),
        "stoppedAt": job.get("stoppedAt"),
        "attempts": [],
        "tags": job.get("tags", {}),
        "parameters": job.get("parameters", {}),
        "timeout": job.get("timeout", {}),
        "retryStrategy": job.get("retryStrategy", {"attempts": 1}),
        "platformCapabilities": [],
        "container": {
            "exitCode": job.get("exitCode"),
            "logStreamName": f"corral/default/{job['jobId']}",
        },
    }


def _now_ms() -> int:
    return int(time.time() * 1000)
