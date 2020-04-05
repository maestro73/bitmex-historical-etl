import os

import google.auth
from google.cloud import bigquery

from .constants import (
    BIGQUERY_DATASET,
    BIGQUERY_INTERMEDIATE_TABLE_NAME,
    BIGQUERY_LOCATION,
    BIGQUERY_TABLE_NAME,
)

HISTORICAL_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("index", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("sequence", "INTEGER", "REQUIRED"),
]

COMBINED_TRADE_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING", "REQUIRED"),
    bigquery.SchemaField("date", "DATE", "REQUIRED"),
    bigquery.SchemaField("timestamp", "TIMESTAMP", "REQUIRED"),
    bigquery.SchemaField("price", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("averagePrice", "FLOAT", "REQUIRED"),
    bigquery.SchemaField("volume", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("tickRule", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("exponent", "INTEGER", "REQUIRED"),
    bigquery.SchemaField("notional", "FLOAT", "REQUIRED"),
]


def get_schema_columns(schema):
    return [field.name for field in schema]


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

        self.dataset = os.environ[BIGQUERY_DATASET]

    def get_table_id(self, table_name):
        return f"{self.dataset}.{table_name}"

    def table_exists(self, table_name):
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("table", "STRING", table_name)
            ]
        )
        query = f"""
            SELECT size_bytes FROM {self.dataset}.__TABLES__
            WHERE table_id = @table;
        """
        job = self.bq.query(query, job_config=job_config)
        rows = job.result()
        return len(list(rows)) > 0

    def create_table(self, table_name, schema):
        dataset_ref = self.bq.dataset(self.dataset)
        table_ref = dataset_ref.table(table_name)
        table = bigquery.Table(table_ref, schema=schema)
        # Partition on date.
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field="date"
        )
        table = self.bq.create_table(table)

    def sequence_query(self):
        table_name = os.environ[BIGQUERY_INTERMEDIATE_TABLE_NAME]
        table_id = self.get_table_id(table_name)
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", self.date)]
        )
        # Query by partition.
        sql = f"""
            SELECT * EXCEPT (date, sequence)
            FROM {table_id}
            WHERE date = @date
            ORDER BY symbol, timestamp, index;
        """
        query = self.bq.query(sql, job_config=job_config)
        return query.result().to_dataframe()

    def combine_trade_query(self):
        table_id = self.get_table_id(os.environ[BIGQUERY_INTERMEDIATE_TABLE_NAME])
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", self.date)]
        )
        # Query by partition.
        sql = f"""
            SELECT symbol, timestamp, price, volume, tickRule, sequence
            FROM {table_id}
            WHERE date = @date
            ORDER BY symbol, timestamp, sequence;
        """
        query = self.bq.query(sql, job_config=job_config)
        return query.result().to_dataframe()

    def calculation_query(self):
        table_id = self.get_table_id(os.environ[BIGQUERY_TABLE_NAME])
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("date", "DATE", self.date)]
        )
        # Query by partition.
        sql = f"""
            SELECT symbol, timestamp, price, volume, tickRule
            FROM {table_id}
            WHERE date = @date
            ORDER BY symbol, timestamp;
        """
        query = self.bq.query(sql, job_config=job_config)
        return query.result().to_dataframe()

    def load(self, table_name, schema, data_frame):
        if not self.table_exists(table_name):
            self.create_table(table_name, schema)
        columns = get_schema_columns(schema)
        data_frame = data_frame[columns]
        # Partition by date.
        table_id = self.get_table_id(table_name)
        decorator = self.date.strftime("%Y%m%d")
        partition = f"{table_id}${decorator}"
        job = self.bq.load_table_from_dataframe(
            data_frame,
            partition,
            job_config=bigquery.LoadJobConfig(
                schema=schema, write_disposition="WRITE_TRUNCATE"
            ),
        )
        job.result()

    def delete_table(self, table_name):
        if self.table_exists(table_name):
            table_id = self.get_table_id(table_name)
            self.bq.delete_table(table_id)
