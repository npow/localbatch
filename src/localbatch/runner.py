"""Docker-based job runner. Executes Batch jobs as local Docker containers."""

import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)

# Simulate AWS Batch state transitions with a small delay so pollers see them.
_TRANSITION_DELAY = 0.05  # seconds


def _merge_env(base: list, overrides: list) -> dict:
    """Merge Batch-style [{name, value}] env lists, overrides win."""
    env = {}
    for e in base:
        env[e["name"]] = e.get("value", "")
    for e in overrides:
        # containerOverrides may use value or value.$ (Step Functions ref)
        env[e["name"]] = e.get("value", e.get("value.$", ""))
    return env


class DockerRunner:
    def __init__(
        self,
        store,
        host_addr: str,
        port: int,
        inject_env: Optional[dict] = None,
    ):
        self.store = store
        self.host_addr = host_addr
        self.port = port
        self.inject_env = inject_env or {}
        self._client = None
        self._connect()

    def _connect(self):
        try:
            import docker

            self._client = docker.from_env()
            self._client.ping()
            logger.info("Docker connection established")
        except Exception as exc:
            logger.warning("Docker unavailable — jobs will fail immediately: %s", exc)
            self._client = None

    def submit(self, job: dict):
        """Kick off a background thread to run a job."""
        t = threading.Thread(target=self._run, args=(job,), daemon=True)
        t.start()

    def terminate(self, job_id: str, reason: str):
        jobs = self.store.get_jobs(job_ids=[job_id])
        if not jobs:
            return
        container_id = jobs[0].get("_container_id")
        self.store.update_job(
            job_id,
            status="FAILED",
            statusReason=reason,
            stoppedAt=_now_ms(),
        )
        if container_id and self._client:
            try:
                self._client.containers.get(container_id).kill()
            except Exception:
                pass

    # ------------------------------------------------------------------

    def _run(self, job: dict):
        job_id = job["jobId"]

        # Simulate AWS Batch lifecycle transitions
        for status in ("PENDING", "RUNNABLE", "STARTING"):
            time.sleep(_TRANSITION_DELAY)
            self.store.update_job(job_id, status=status)

        self.store.update_job(job_id, startedAt=_now_ms())

        if not self._client:
            self._fail(job_id, "Docker unavailable")
            return

        job_def = self.store.get_job_definition_by_arn(job["jobDefinition"])
        if not job_def:
            self._fail(job_id, f"Job definition not found: {job['jobDefinition']}")
            return

        container_props = job_def.get("containerProperties", {})
        overrides = job.get("containerOverrides", {})

        image = container_props.get("image", "")
        command = overrides.get("command") or container_props.get("command")
        env = _merge_env(
            container_props.get("environment", []),
            overrides.get("environment", []),
        )

        # Caller-supplied env vars (e.g. MinIO credentials, metadata service
        # URL) that every container should inherit.  inject_env wins over
        # job-specific values so that infrastructure URLs (host.docker.internal)
        # always take precedence over whatever the submitter passes (e.g.
        # localhost:9000 on the host vs host.docker.internal:9000 in container).
        merged = dict(env)
        merged.update(self.inject_env)
        env = merged

        # Inject standard AWS Batch environment variables that real Batch
        # injects automatically.  Metaflow uses AWS_BATCH_JOB_ATTEMPT to
        # compute --retry-count (as $((AWS_BATCH_JOB_ATTEMPT-1))), so it
        # must be ≥ 1 or the arithmetic produces -1 which click rejects.
        # AWS_BATCH_CE_NAME and AWS_BATCH_JQ_NAME are read by Metaflow's
        # batch_decorator.task_pre_step to populate run metadata.
        env.setdefault("AWS_BATCH_JOB_ID", job_id)
        env.setdefault("AWS_BATCH_JOB_ATTEMPT", "1")
        env.setdefault("AWS_BATCH_CE_NAME", "localbatch-local")
        env.setdefault("AWS_BATCH_JQ_NAME", job.get("jobQueue", "localbatch-default"))
        env.setdefault("AWS_EXECUTION_ENV", "AWS_ECS_EC2")

        # Inject the fake ECS container metadata endpoint so Metaflow can
        # discover the (synthetic) CloudWatch log stream name.
        env["ECS_CONTAINER_METADATA_URI_V4"] = (
            f"http://{self.host_addr}:{self.port}/metadata/{job_id}"
        )

        container = None
        try:
            container = self._client.containers.run(
                image=image,
                command=command,
                environment=env,
                detach=True,
                remove=False,
                # Make the host reachable as host.docker.internal on Linux too
                extra_hosts={"host.docker.internal": "host-gateway"},
            )
            self.store.update_job(
                job_id, status="RUNNING", _container_id=container.id
            )

            result = container.wait(timeout=7200)
            exit_code = result.get("StatusCode", -1)

            # Capture container logs for debugging
            try:
                logs = container.logs(stdout=True, stderr=True, tail=200)
                log_text = logs.decode("utf-8", errors="replace").strip()
                if exit_code != 0:
                    logger.error(
                        "Job %s failed (exit %d). Container logs:\n%s",
                        job_id, exit_code, log_text,
                    )
                else:
                    logger.debug("Job %s logs:\n%s", job_id, log_text)
            except Exception as log_exc:
                logger.warning("Could not capture logs for %s: %s", job_id, log_exc)

            if exit_code == 0:
                self.store.update_job(
                    job_id, status="SUCCEEDED", stoppedAt=_now_ms()
                )
            else:
                self.store.update_job(
                    job_id,
                    status="FAILED",
                    statusReason=f"Container exited with code {exit_code}",
                    exitCode=exit_code,
                    stoppedAt=_now_ms(),
                )

        except Exception as exc:
            logger.exception("Job %s raised an exception", job_id)
            self._fail(job_id, str(exc)[:500])
        finally:
            if container:
                try:
                    container.remove(force=True)
                except Exception:
                    pass

    def _fail(self, job_id: str, reason: str):
        self.store.update_job(
            job_id, status="FAILED", statusReason=reason, stoppedAt=_now_ms()
        )


def _now_ms() -> int:
    return int(time.time() * 1000)
