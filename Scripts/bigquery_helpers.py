from __future__ import annotations

"""Utility helpers for uploading dataframes to BigQuery.

This module centralises the BigQuery upload logic so that different
processing scripts (e.g. IMTP, PPU, HJ) can share the same behaviour.
"""

from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from config import settings

PROJECT_ID = settings.gcp.project_id
DATASET_ID = settings.gcp.dataset_id
CREDENTIALS_FILE = settings.gcp.credentials_file

try:
    _credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
    bq_client = bigquery.Client(credentials=_credentials, project=PROJECT_ID)
    print("Successfully loaded GCP credentials and BigQuery client.")
except Exception as e:  # pragma: no cover - used for runtime feedback
    print(f"ERROR: Could not load credentials. {e}")
    bq_client = None


def upload_to_bigquery(df: pd.DataFrame, table_name: str) -> bool:
    """Upload ``df`` to the BigQuery table ``table_name``.

    Columns that are not present in the destination table's schema are
    dropped before upload. Returns ``True`` on success.
    """
    if df.empty:
        print(f"DataFrame for {table_name} is empty. Skipping upload.")
        return False
    if bq_client is None:
        print("BigQuery client not available. Cannot upload.")
        return False

    table_ref = bq_client.dataset(DATASET_ID).table(table_name)
    # Retrieve existing schema to restrict columns
    table = bq_client.get_table(table_ref)
    existing_fields = {field.name for field in table.schema}
    df = df[[col for col in df.columns if col in existing_fields]]

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print("Upload successful!")
        return True
    except Exception as e:
        print(f"An error occurred during the BigQuery upload: {e}")
        return False