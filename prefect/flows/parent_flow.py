from postgres_to_gcs import postgres_to_gcs_flow
from prepare_move_to_bq import prepare_move_to_bq_flow
from bq_procedures import bq_procedures_flow
from prefect_email import EmailServerCredentials, email_send_message
from datetime import datetime
from prefect import task, flow


@flow(name = "Parent Flow", description = "Parent Flow of all Pipeline Subflows")
def parent_flow():
    email_credentials_block = EmailServerCredentials.load("email-notifier")
    dates_lst = [datetime.now().strftime("%Y%m%d"), datetime.now().strftime("%Y%m%d"), datetime.now().strftime("%Y%m%d")]
    try:
        postgres_to_gcs_flow(dates_lst)
        prepare_move_to_bq_flow(dates_lst[0])
        bq_procedures_flow()
        email_send_message(
            email_server_credentials=email_credentials_block,
            subject=f"online store pipeline status",
            msg=f"Flow Executed Successfully on {dates_lst[0]} data.",
            email_to=email_credentials_block.username
        )
    except Exception as ex:
        email_send_message(
            email_server_credentials=email_credentials_block,
            subject=f"online store pipeline status",
            msg=f"Flow failed due to {ex} on {dates_lst[0]} data",
            email_to=email_credentials_block.username
        )
if __name__ == "__main__":
    parent_flow()