from prefect import task, flow
import subprocess

@task(name = "RunAggOrdersFactJob", description = "Run p_agg_orders_fact procedure in BigQuery",
      log_prints = True, tags = ["stored-procedure", "bigquery"])
def call_agg_orders_bq() -> None:
    print("Running Agg Orders Fact Procedure Procedure")
    command = "bq query --use_legacy_sql=false 'CALL online_store_data.p_agg_orders_fact()'"
    process = subprocess.run(command, shell=True, capture_output=True)
    if process.returncode != 0:
        print('Task Failed')
        print('Error message : ' + str(process.stderr))
    else:
        print("Procedure Finised Successfully")

@task(name = "RunRFMJob", description = "Run p_rfm_analysis procedure in BigQuery",
      log_prints = True, tags = ["stored-procedure", "bigquery"])
def call_rfm_bq() -> None:
    print("Running RFM Analysis Procedure")
    command = "bq query --use_legacy_sql=false 'CALL online_store_data.p_rfm_analysis()'"
    process = subprocess.run(command, shell=True, capture_output=True)
    if process.returncode != 0:
        print('Task Failed')
        print('Error message : ' + str(process.stderr))
    else:
        print("Procedure Finised Successfully")
    
@flow(name = "BQProceduresCaller", description = "flow of calling procedures",
      log_prints = True)
def bq_procedures_flow():
    call_agg_orders_bq()
    call_rfm_bq()
    