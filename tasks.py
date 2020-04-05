import os

from invoke import task

from bitmex_historical_etl.constants import BIGQUERY_LOCATION, BIGQUERY_TABLE_NAME
from bitmex_historical_etl.utils import get_deploy_env_vars, set_environment

set_environment()


@task
def deploy(c):
    region = os.environ.get(BIGQUERY_LOCATION)
    topic = os.environ.get(BIGQUERY_TABLE_NAME)
    # Because Error: memory limit exceeded. Function invocation was interrupted.
    memory = 2048
    timeout = 540
    env_vars = get_deploy_env_vars()
    cmd = f"""
        gcloud functions deploy bitmex \
            --region={region} \
            --memory={memory}MB \
            --timeout={timeout}s \
            --runtime=python37 \
            --entry-point=bitmex \
            --set-env-vars={env_vars} \
            --trigger-topic={topic}
    """
    c.run(cmd)
