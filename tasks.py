import json
import os

from invoke import task

from bitmex_historical_etl.constants import BIGQUERY_LOCATION, PUBSUB_TOPIC
from bitmex_historical_etl.utils import get_deploy_env_vars, set_environment

set_environment()


@task
def deploy_function(c):
    region = os.environ.get(BIGQUERY_LOCATION)
    topic = os.environ.get(PUBSUB_TOPIC)
    # Because Error: memory limit exceeded. Function invocation was interrupted.
    memory = 2048
    timeout = 540
    env_vars = get_deploy_env_vars()
    cmd = f"""
        gcloud functions deploy bitmex-historical \
            --region={region} \
            --memory={memory}MB \
            --timeout={timeout}s \
            --runtime=python37 \
            --entry-point=get_bitmex_historical \
            --set-env-vars={env_vars} \
            --trigger-topic={topic}
    """
    c.run(cmd)


@task
def create_scheduler(c):
    topic = os.environ.get(PUBSUB_TOPIC)
    message_body = json.dumps({})
    command = f"""
        gcloud scheduler jobs create pubsub bitmex-historical \
            --schedule="*/10 * * * *" \
            --topic={topic} \
            --message-body='{message_body}'
    """
    c.run(command)


@task
def deploy(c):
    for t in (deploy_function, create_scheduler):
        t(c)
