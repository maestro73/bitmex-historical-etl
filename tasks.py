import os

from invoke import task

from bitmex_historical_etl.constants import BIGQUERY_LOCATION, PUBSUB_TOPIC
from bitmex_historical_etl.utils import get_deploy_env_vars, set_environment

set_environment()


@task
def deploy_function(c):
    region = os.environ.get(BIGQUERY_LOCATION)
    topic = os.environ.get(PUBSUB_TOPIC)
    env_vars = get_deploy_env_vars()
    cmd = f"""
        gcloud functions deploy get-bitmex-historical \
            --region={region} \
            --memory=512MB \
            --runtime=python37 \
            --entry-point=get_bitmex_historical \
            --set-env-vars={env_vars} \
            --trigger-topic={topic}
    """
    c.run(cmd)


@task
def create_pubsub_topic(c):
    topic = os.environ.get(PUBSUB_TOPIC)
    cmd = f"""
        gcloud pubsub topics create {topic}
    """
    c.run(cmd)


@task
def create_scheduler(c):
    topic = os.environ.get(PUBSUB_TOPIC)
    command = f"""
        gcloud scheduler jobs create pubsub bitmex-historical \
            --schedule="*/5 * * * *" \
            --topic={topic} \
    """
    c.run(command)


@task
def deploy(c):
    for t in (deploy_function, create_pubsub_topic, create_scheduler):
        t(c)
