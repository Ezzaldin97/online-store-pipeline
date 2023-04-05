# Orchestration of Data Pipeline

## Objective:

Create Fully Automated Pipeline from start to finish:

- starts from pull data from postgres database.
- ingesting data to GCS on a daily basis.
- creating transformation pipeline using Stored Procedures in BigQuery.

## Potential Steps to Run the Orchestration flows:

- Make Sure to Run Postgres Docker Image of Fake Data Generator (Daily Data Source), for more info. check REDAME.md in fake-data-generator Directory.
- start by running ` make venv ` to create a python virtual environment for the orchestration part to avoid in errors.
- Configure a SQLAlchemy Connector Block of Postgres Database (Data Source).
- Configure a GCP Credential Block.
- Configure a GCS Bucket Block.
- Configure a Mail Block (Optional to Send Notifications), Make Sure to Generate an App Password for Username, for more info see this example: https://www.lifewire.com/manage-app-passwords-imap-pop-yahoo-1174448
- run the flow using ` make run `.

## Pipeline:

- `postgres_to_gcs_flow()` this flow responsible for getting daily data from postgres database, and ingest the data to GCS as is, no transformation here.
- `prepare_move_to_bq_flow()` this flow prepare the data to move the data to big query.
- `bq_procedures_flow()` this flow call the stored procedures, for more info about stored procedures please check README in sql directory.