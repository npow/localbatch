"""CLI entry point: python -m localbatch  or  localbatch"""

import logging

import click
import uvicorn

from .runner import DockerRunner
from .server import create_app
from .store import Store


@click.command()
@click.option("--host", default="0.0.0.0", show_default=True, help="Bind address")
@click.option("--port", default=8000, show_default=True, help="Port to listen on")
@click.option(
    "--docker-host",
    default="host.docker.internal",
    show_default=True,
    envvar="LOCALBATCH_DOCKER_HOST",
    help=(
        "Address that containers use to reach localbatch. "
        "On Linux, try 172.17.0.1 if host.docker.internal doesn't resolve."
    ),
)
@click.option(
    "--queue",
    default="localbatch-default",
    show_default=True,
    envvar="LOCALBATCH_QUEUE",
    help="Default job queue name (pre-created on startup)",
)
@click.option(
    "--inject-env",
    "inject_envs",
    multiple=True,
    metavar="KEY=VALUE",
    envvar="LOCALBATCH_INJECT_ENVS",
    help=(
        "Extra env var to inject into every container (repeatable). "
        "Useful for forwarding MinIO credentials or the metadata service URL. "
        "Example: --inject-env AWS_ACCESS_KEY_ID=rootuser"
    ),
)
@click.option(
    "--log-level",
    default="info",
    show_default=True,
    type=click.Choice(["debug", "info", "warning", "error"]),
)
def main(host, port, docker_host, queue, inject_envs, log_level):
    """
    localbatch -- local AWS Batch emulator

    Runs an AWS Batch-compatible HTTP server and executes submitted jobs
    as local Docker containers.

    Point your Metaflow setup at localbatch:

    \b
      export METAFLOW_BATCH_JOB_QUEUE=localbatch-default
      export METAFLOW_BATCH_CLIENT_PARAMS='{"endpoint_url":"http://localhost:8000"}'
      export AWS_DEFAULT_REGION=us-east-1
      export AWS_ACCESS_KEY_ID=test
      export AWS_SECRET_ACCESS_KEY=test
    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    parsed_inject = {}
    for item in inject_envs:
        if "=" not in item:
            raise click.BadParameter(
                f"must be KEY=VALUE, got: {item!r}", param_hint="--inject-env"
            )
        k, v = item.split("=", 1)
        parsed_inject[k] = v

    store = Store(queue_name=queue)
    runner = DockerRunner(store, host_addr=docker_host, port=port, inject_env=parsed_inject)
    app = create_app(store, runner)

    click.echo(f"localbatch listening on http://{host}:{port}")
    click.echo(f"  default queue : {queue}")
    click.echo(f"  docker host   : {docker_host}")
    if parsed_inject:
        click.echo(f"  injecting     : {', '.join(parsed_inject)}")
    click.echo(f"  health check  : http://localhost:{port}/health")

    uvicorn.run(app, host=host, port=port, log_level=log_level)


if __name__ == "__main__":
    main()
