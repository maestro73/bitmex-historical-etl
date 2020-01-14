import datetime
import os
from unittest.mock import Mock

import firebase_admin
import google.auth
import pytest
from firebase_admin import firestore
from firebase_admin.credentials import Certificate
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

import main
from bitmex_historical_etl.constants import (
    BIGQUERY_DATASET,
    BIGQUERY_LOCATION,
    BIGQUERY_TABLE_NAME,
    FIREBASE_ADMIN_CREDENTIALS,
    FIRESTORE_COLLECTION,
)
from bitmex_historical_etl.utils import (
    base64_encode_dict,
    get_bitmex_s3_data_url,
    get_delta,
    set_environment,
)

mock_context = Mock()
mock_context.event_id = "1"
mock_context.timestamp = datetime.datetime.utcnow().isoformat()


def cleanup_bigquery():
    table_name = os.environ[BIGQUERY_TABLE_NAME]
    assert "_test" in table_name
    credentials, project_id = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    bq = bigquery.Client(
        credentials=credentials,
        project=project_id,
        location=os.environ.get(BIGQUERY_LOCATION, None),
    )
    dataset = os.environ[BIGQUERY_DATASET]
    table_id = f"{dataset}.{table_name}"
    try:
        bq.delete_table(table_id)
    except NotFound:
        pass


def cleanup_firestore():
    ref = os.environ[FIRESTORE_COLLECTION]
    assert "-test" in ref
    if "FIREBASE_INIT" not in os.environ:
        certificate = Certificate(os.environ[FIREBASE_ADMIN_CREDENTIALS])
        firebase_admin.initialize_app(certificate)
        os.environ["FIREBASE_INIT"] = "true"
    fs = firestore.client()
    docs = fs.collection(ref).stream()
    for doc in docs:
        doc.reference.delete()


def cleanup():
    cleanup_bigquery()
    cleanup_firestore()


@pytest.fixture(autouse=True)
def setenv():
    # Setup
    set_environment()
    cleanup()
    yield True
    cleanup()


def test_bitmex_historical_etl(capsys):
    date = "2016-05-13"
    data = {"date": date}
    d = {"data": base64_encode_dict(data)}
    main.get_bitmex_historical(d, mock_context)
    out, err = capsys.readouterr()
    assert f"{date} OK" in out


def test_bitmex_historical_etl_404(capsys):
    two_days_from_now = get_delta(days=2)
    date = two_days_from_now.isoformat()
    data = {"date": date}
    d = {"data": base64_encode_dict(data)}
    main.get_bitmex_historical(d, mock_context)
    url = get_bitmex_s3_data_url(two_days_from_now)
    out, err = capsys.readouterr()
    assert f"No data: {url}" in out
