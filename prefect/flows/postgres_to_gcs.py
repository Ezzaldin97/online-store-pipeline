import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_sqlalchemy import SqlAlchemyConnector
import os
from pathlib import Path
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
from typing import List

@task(name = "FetchOrdersData", description = "Get Orders Data From Postgres DB as Store it as Pandas DataFrame.",
      retries = 2, tags = ["Fetch", "OrdersData", "PostgresDB"], cache_key_fn = task_input_hash,
      cache_expiration = timedelta(minutes = 15), log_prints=True)
def fetch_orders_data(conn, date:str) -> pd.DataFrame:
    orders_df = pd.read_sql_table(table_name = f"orders{date}", con = conn, schema = "store")
    print(f"Orders Data Fetched Successfully, Data Shape: {orders_df.shape}")
    return orders_df

@task(name = "FetchCutomersData", description = "Get Customers Data From Postgres DB as Store it as Pandas DataFrame.",
      retries = 2, tags = ["Fetch", "CustomersData", "PostgresDB"], cache_key_fn = task_input_hash,
      cache_expiration = timedelta(minutes = 15), log_prints=True)
def fetch_customers_data(conn, date:str) -> pd.DataFrame:
    custs_df = pd.read_sql_table(table_name = f"customers{date}", con = conn, schema = "store")
    print(f"Customers Data Fetched Successfully, Data shape: {custs_df.shape}")
    return custs_df

@task(name = "FetchItemsData", description = "Get Items Data From Postgres DB as Store it as Pandas DataFrame.",
      retries = 2, tags = ["Fetch", "ItemsData", "PostgresDB"], cache_key_fn = task_input_hash,
      cache_expiration = timedelta(minutes = 15), log_prints=True)
def fetch_items_data(conn, date:str) -> pd.DataFrame:
    items_df = pd.read_sql_table(table_name = f"items{date}", con = conn, schema = "store")
    print(f"Items Data Fetched Successfully, Data Shape: {items_df.shape}")
    return items_df

@task(name = "LoadToGCS", description = "Load Pandas DataFrames to GCS Bucket",
      retries = 2, tags = ["Load", "Data", "GCS"], log_prints = True)
def load_to_gcs(df:pd.DataFrame, file_name:str) -> None:
    df = df.astype(str)
    gcs_block = GcsBucket.load("online-store-datalake")
    print("Upload DataFrame to GCS")
    gcs_block.upload_from_dataframe(
        df = df,
        to_path = f"raw/{file_name}.parquet",
        serialization_format="parquet_snappy"
    )
    print("Data Uploaded raw Directory in GCS")

@flow(name = "PostgresToGCS", description = "Main Flow of Extract Data from Source to GCS",
      log_prints = True)
def postgres_to_gcs_flow(dates_lst:List[str]=[datetime.now().strftime("%Y%m%d"), datetime.now().strftime("%Y%m%d"), datetime.now().strftime("%Y%m%d")]) -> None:
    postgres_connector = SqlAlchemyConnector.load("postgres-db")
    engine = postgres_connector.get_client(client_type="engine")
    with engine.connect() as conn:
        orders_df = fetch_orders_data(conn, dates_lst[0])
        custs_df = fetch_customers_data(conn, dates_lst[1])
        items_df = fetch_items_data(conn, dates_lst[2])
    print("Load Orders Data to GCS")
    orders_data_gcs_path = load_to_gcs(orders_df, f"orders{dates_lst[0]}")
    print(f"Data Uploaded to {orders_data_gcs_path}")
    print("Load Customers Data to GCS")
    cust_data_gcs_path = load_to_gcs(custs_df, f"customers{dates_lst[1]}")
    print(f"Data Loaded to {cust_data_gcs_path}")
    print("Load Items Data to GCS")
    items_data_gcs_path = load_to_gcs(items_df, f"items{dates_lst[2]}")
    print(f"Data Loaded to {items_data_gcs_path}")
    print("All Data Loaded")