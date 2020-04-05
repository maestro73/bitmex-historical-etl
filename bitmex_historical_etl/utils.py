import base64
import datetime
import json
import os
from pathlib import Path

from .constants import (
    BIGQUERY_INTERMEDIATE_TABLE_NAME,
    BIGQUERY_TABLE_NAME,
    BITMEX_S3,
    GCP_APPLICATION_CREDENTIALS,
    PRODUCTION_ENV_VARS,
)


def set_environment():
    with open("env.yaml", "r") as env:
        for line in env:
            key, value = line.split(": ")
            v = value.strip()
            if key in GCP_APPLICATION_CREDENTIALS:
                path = Path.cwd().parents[0] / "keys" / v
                v = str(path.resolve())
            if key in (BIGQUERY_TABLE_NAME, BIGQUERY_INTERMEDIATE_TABLE_NAME):
                if "PYTEST_CURRENT_TEST" in os.environ:
                    v = f"{v}_test"
            os.environ[key] = v


def get_deploy_env_vars():
    env_vars = []
    with open("env.yaml", "r") as env:
        for line in env:
            key, value = line.split(": ")
            v = value.strip()
            if key in PRODUCTION_ENV_VARS:
                env_vars.append(f"{key}={v}")
    return ",".join(env_vars)


def is_local():
    return all([os.environ.get(key, None) for key in GCP_APPLICATION_CREDENTIALS])


def get_delta(minutes=0, days=0):
    assert minutes or days
    today = datetime.datetime.utcnow()
    delta = today + datetime.timedelta(minutes=minutes, days=days)
    return delta.date()


def get_bitmex_s3_data_url(date):
    date_string = date.strftime("%Y%m%d")
    return f"{BITMEX_S3}{date_string}.csv.gz"


def base64_encode_dict(data):
    d = json.dumps(data).encode()
    return base64.b64encode(d)
