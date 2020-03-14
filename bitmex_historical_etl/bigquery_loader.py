import os

import google.auth
from google.cloud import bigquery

from .constants import BIGQUERY_DATASET, BIGQUERY_LOCATION, BIGQUERY_TABLE_NAME


class BigQueryLoader:
    def __init__(self, date):
        self.date = date

        credentials, project_id = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        self.bq = bigquery.Client(
            credentials=credentials,
            project=project_id,
            location=os.environ.get(BIGQUERY_LOCATION, None),
        )

        self.schema = [
            bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
            bigquery.SchemaField("date", "DATE", "REQUIRED"),
            bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
            bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
            bigquery.SchemaField("volume", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
            bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
        ]

        self.dataset = os.environ[BIGQUERY_DATASET]
        self.table_name = os.environ[BIGQUERY_TABLE_NAME]

        if not self._table_exists():
            self._create_table()

    def _table_exists(self):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table", "STRING", self.table_name)
            ]
        )
        query = f"""
            SELECT size_bytes FROM {self.dataset}.__TABLES__
            WHERE table_id = @table;
        """
        job = self.bq.query(query, job_config=job_config)
        rows = job.result()
        return len(list(rows)) > 0

    def _create_table(self):
        dataset_ref = self.bq.dataset(self.dataset)
        table_ref = dataset_ref.table(self.table_name)
        table = bigquery.Table(table_ref, schema=self.schema)
        # Partition on date.
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )
        table = self.bq.create_table(table)

    def load(self, data_frame):
        decorator = self.date.strftime("%Y%m%d")
        table_id = f"{self.dataset}.{self.table_name}"
        # Because BigQuery doesn't use or support indexes.
        partition = f"{table_id}${decorator}"
        job = self.bq.load_table_from_dataframe(
            data_frame,
            partition,
            job_config=bigquery.LoadJobConfig(
                schema=self.schema, write_disposition="WRITE_TRUNCATE"
            ),
        )
        job.result()
