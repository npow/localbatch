"""FastAPI application implementing the AWS Batch REST API surface used by Metaflow."""

import time
import uuid
from typing import Optional

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

    # ------------------------------------------------------------------
    # Queues & Compute Environments
    # ------------------------------------------------------------------

    @app.post("/v1/describejobqueues")
    async def describe_job_queues(request: Request):
        body = await request.json()
        return {
            "jobQueues": store.get_queues(names=body.get("jobQueues")),
            "nextToken": None,
        }

    @app.post("/v1/describecomputeenvironments")
    async def describe_compute_environments(request: Request):
        body = await request.json()
        return {
            "computeEnvironments": store.get_compute_envs(
                names=body.get("computeEnvironments")
            ),
            "nextToken": None,
        }

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
