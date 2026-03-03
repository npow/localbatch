# corral

[![CI](https://github.com/npow/corral/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/corral/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/corral)](https://pypi.org/project/corral/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

Test your AWS Batch workloads locally without an AWS account.

## The problem

You write a step that runs on `@batch`. Testing it means pushing code, waiting for ECR, paying for EC2, and debugging through CloudWatch — a cycle that takes minutes per iteration. LocalStack supports Batch only in its paid Pro tier. Moto mocks the API but never runs your container. There is no free, real AWS Batch emulator.

## Quick start

```bash
pip install corral
corral --port 8000
```

Point any AWS Batch client at it:

```bash
export AWS_ENDPOINT_URL_BATCH=http://localhost:8000
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws batch describe-job-queues   # → corral-default queue
```

## Install

```bash
# From PyPI
pip install corral

# From source
pip install -e ~/code/corral
```

## Usage

### Run a job via boto3

```python
import boto3

batch = boto3.client(
    "batch",
    endpoint_url="http://localhost:8000",
    region_name="us-east-1",
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

batch.register_job_definition(
    jobDefinitionName="my-job",
    type="container",
    containerProperties={
        "image": "alpine:latest",
        "command": ["echo", "hello from corral"],
        "resourceRequirements": [
            {"type": "VCPU", "value": "1"},
            {"type": "MEMORY", "value": "256"},
        ],
    },
)

resp = batch.submit_job(
    jobName="my-run",
    jobQueue="corral-default",
    jobDefinition="my-job",
)
print(resp["jobId"])
```

### Run a Metaflow @batch step locally

```bash
export METAFLOW_BATCH_JOB_QUEUE=corral-default
export METAFLOW_BATCH_CLIENT_PARAMS='{"endpoint_url":"http://localhost:8000"}'

python my_flow.py run
```

### Inject environment variables into every container

Useful for forwarding MinIO credentials or a local metadata service URL:

```bash
corral \
  --inject-env AWS_ACCESS_KEY_ID=minioadmin \
  --inject-env AWS_SECRET_ACCESS_KEY=minioadmin \
  --inject-env AWS_ENDPOINT_URL_S3=http://host.docker.internal:9000
```

## How it works

corral runs a FastAPI server that implements the AWS Batch REST API surface used by boto3 and the AWS CLI. When a job is submitted, corral pulls the container image and runs it with `docker run`. Job status transitions (`SUBMITTED → PENDING → RUNNABLE → STARTING → RUNNING → SUCCEEDED/FAILED`) are driven by the real container exit code.

A fake ECS Container Metadata endpoint (`/metadata/{job_id}/task`) is injected into every container via `ECS_CONTAINER_METADATA_URI_V4` so that frameworks like Metaflow can read a synthetic CloudWatch log stream name without hitting real AWS.

## Configuration

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--host` | — | `127.0.0.1` | Bind address |
| `--port` | — | `8000` | Listen port |
| `--queue` | `CORRAL_QUEUE` | `corral-default` | Default job queue name |
| `--docker-host` | `CORRAL_DOCKER_HOST` | `host.docker.internal` | Hostname containers use to reach the host |
| `--inject-env KEY=VAL` | — | — | Env var forwarded into every container (repeatable) |
| `--log-level` | — | `info` | uvicorn log level |

## Development

```bash
git clone https://github.com/npow/corral
pip install -e corral
pytest test/unit/corral/ -m "not docker"   # API tests, no Docker required
pytest test/unit/corral/                   # all tests (requires Docker)
```

Note: the integration tests live in the [Metaflow](https://github.com/Netflix/metaflow) repository under `test/unit/corral/`.

## License

[Apache 2.0](LICENSE)
