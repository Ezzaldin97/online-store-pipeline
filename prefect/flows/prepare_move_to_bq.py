from prefect_gcp.cloud_storage import GcsBucket
import warnings
from datetime import datetime
import random
import pandas as pd
from prefect import task, flow
from typing import List
import pandas as pd
import gcsfs
from prefect_gcp import GcpCredentials


# create a user defined function to create a unique item code.
def item_code(item):
    code = ""
    for idx, val in enumerate(item):
        if idx == 0 or idx == len(item)-1:
            code+=str(ord(val))
        else:
            code+=val
    return code

@task(name = "PrepareData", description = "Prepare raw Data to Move to BigQuery",
      log_prints = True, tags = ["Schema", "raw", "data"])
def prepare_data(date_str_format:str):
    gcs_block = GcsBucket.load("online-store-datalake")
    gcs_fs = gcsfs.GCSFileSystem(project='online-store-382421')
    print("Prepare Orders Data")
    orders = pd.read_parquet(f"gs://store_data_lake_online-store-382421/raw/orders{date_str_format}.parquet.snappy")
    orders["customer_id"] = orders["customer_id"].astype(int)
    orders["item"] = orders["item"].astype(str)
    orders["created_at"] = pd.to_datetime(orders["created_at"], format = "%Y-%m-%d")
    print("Prepare Items Data")
    items = pd.read_parquet(f"gs://store_data_lake_online-store-382421/raw/items{date_str_format}.parquet.snappy")
    items["item"] = items["item"].astype(str)
    items["price"] = items["price"].astype(int)
    items["description"] = items["description"].astype(str)
    items["discount_flag"] = items["discount_flag"].astype(str)
    items["last_day_score"] = items["last_day_score"].astype(float)
    items.drop("last_day_sales", axis = 1, inplace = True)
    print("Add Item ID to Each Item")
    items["iid"] = items["item"].apply(lambda x: item_code(x))
    print("Replace the Item Name in Orders Data by Item ID")
    orders["iid"] = orders["item"].apply(lambda x: item_code(x))
    orders.drop("item", axis = 1, inplace = True)
    print("Prepare Customers Data Schema")
    customers = pd.read_parquet(f"gs://store_data_lake_online-store-382421/raw/customers{date_str_format}.parquet.snappy")
    customers["id"] = customers["id"].astype(int)
    customers["Date of Birth"] = pd.to_datetime(customers["Date of Birth"], format = "%Y-%m-%d")
    customers["country"] = customers["country"].astype(str)
    customers["address"] = customers["address"].astype(str)
    customers["gender"] = customers["gender"].astype(str)
    customers["credit_card_provider"] = customers["credit_card_provider"].astype(str)
    customers["credit_card_number"] = customers["credit_card_number"].astype(int)
    customers["credit_card_expire"] = customers["credit_card_expire"].astype(str)
    customers["credit_card_security_code"] = customers["credit_card_security_code"].astype(int)
    gcs_block.upload_from_dataframe(
        df = orders,
        to_path = f"processed/orders.csv",
        serialization_format="CSV_GZIP"
    )
    gcs_block.upload_from_dataframe(
        df = items,
        to_path = f"processed/items.csv",
        serialization_format="CSV_GZIP"
    )
    gcs_block.upload_from_dataframe(
        df = customers,
        to_path = f"processed/customers.csv",
        serialization_format="CSV_GZIP"
    )
    print("All Data Processed, and Stored Successfully to GCS")

@task(name = "LoadToBQ", description = "Load Processed Data to BigQuery",
      log_prints = True, tags = ["Laod", "Data", "BigQuery"], retries = 2)
def load_to_bq(df:pd.DataFrame, bq_table_name:str) -> None:
    gcp_creds_block = GcpCredentials.load("online-store")
    df.to_gbq(
        project_id = "online-store-382421", credentials=gcp_creds_block.get_credentials_from_service_account(),
        destination_table=f"online-store-382421.staging.{bq_table_name}", chunksize = 100_000,
        if_exists="replace"
    )


@flow(name = "PrepareAndMoveToBQ", description = "Prepare Raw Data and Move to BigQuery",
      log_prints = True)
def prepare_move_to_bq_flow(date:str=datetime.now().strftime("%Y%m%d")):
    prepare_data(date)
    gcs_fs = gcsfs.GCSFileSystem(project='online-store-382421')
    orders_df = pd.read_csv(f"gs://store_data_lake_online-store-382421/processed/orders.csv.gz")
    items_df = pd.read_csv(f"gs://store_data_lake_online-store-382421/processed/items.csv.gz")
    customers_df = pd.read_csv(f"gs://store_data_lake_online-store-382421/processed/customers.csv.gz")
    print("Load Orders Data to BigQuery")
    load_to_bq(orders_df, f"orders")
    print("Load Items Data to BigQuery")
    load_to_bq(items_df, f"items")
    print("Load Customers Data to BigQuery")
    load_to_bq(customers_df, f"customers")