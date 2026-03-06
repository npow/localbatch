# localbatch

[![CI](https://github.com/npow/localbatch/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/localbatch/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/localbatch)](https://pypi.org/project/localbatch/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)

The free AWS Batch emulator that actually runs your containers.

## The problem

Testing AWS Batch jobs means pushing code, waiting on queues, and paying for EC2 — just to find out your container crashes on startup. LocalStack's Batch support is paid. Moto validates API calls but never executes your container. There is no free emulator that runs real workloads.

## Quick start

```bash
pip install localbatch
localbatch --port 8000
```

Point any Batch client at it — no AWS account, no credentials:

```bash
export AWS_ENDPOINT_URL_BATCH=http://localhost:8000
export AWS_DEFAULT_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws batch describe-job-queues
aws batch register-job-definition --job-definition-name hello \
    --type container \
    --container-properties '{"image":"alpine","command":["echo","hello"],"resourceRequirements":[{"type":"VCPU","value":"1"},{"type":"MEMORY","value":"256"}]}'
aws batch submit-job --job-name test --job-queue localbatch-default --job-definition hello
```

## Install

```bash
pip install localbatch          # from PyPI
pip install -e ~/code/localbatch  # from source
```

## Usage

### boto3

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
        "command": ["echo", "hello"],
        "resourceRequirements": [
            {"type": "VCPU", "value": "1"},
            {"type": "MEMORY", "value": "256"},
        ],
    },
)

resp = batch.submit_job(
    jobName="my-run",
    jobQueue="localbatch-default",
    jobDefinition="my-job",
)
print(resp["jobId"])
```

### Inject environment variables into every container

Forward credentials or service URLs into all containers without baking them into your job definition:

```bash
localbatch \
  --inject-env AWS_ACCESS_KEY_ID=minioadmin \
  --inject-env AWS_SECRET_ACCESS_KEY=minioadmin \
  --inject-env AWS_ENDPOINT_URL_S3=http://host.docker.internal:9000
```

### Metaflow

```bash
export METAFLOW_BATCH_JOB_QUEUE=localbatch-default
export METAFLOW_BATCH_CLIENT_PARAMS='{"endpoint_url":"http://localhost:8000"}'

python my_flow.py run
```

## How it works

localbatch starts a FastAPI server that implements the complete [AWS Batch REST API](https://docs.aws.amazon.com/batch/latest/APIReference/API_Operations.html) (all 25 operations). When a job is submitted, localbatch runs the container with the Docker SDK. Job status transitions (`SUBMITTED → PENDING → RUNNABLE → STARTING → RUNNING → SUCCEEDED/FAILED`) reflect the real container exit code.

A fake ECS Container Metadata endpoint is injected via `ECS_CONTAINER_METADATA_URI_V4` so that tooling that reads CloudWatch log stream names works without changes.

## Configuration

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `--host` | — | `127.0.0.1` | Bind address |
| `--port` | — | `8000` | Listen port |
| `--queue` | `LOCALBATCH_QUEUE` | `localbatch-default` | Default job queue name |
| `--docker-host` | `LOCALBATCH_DOCKER_HOST` | `host.docker.internal` | Hostname containers use to reach the host |
| `--inject-env KEY=VAL` | — | — | Env var forwarded into every container (repeatable) |
| `--log-level` | — | `info` | uvicorn log level |

## Development

```bash
git clone https://github.com/npow/localbatch
pip install -e localbatch pytest
pytest  # import smoke tests
```

## License

[Apache 2.0](LICENSE)
