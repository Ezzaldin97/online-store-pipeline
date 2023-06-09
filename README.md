# Online Store Data Pipeline

![](imgs/pipeline.PNG)

## Description:

Fully Automated Pipeline to Manage the process of getting Raw Data from Source Systmes, Ingestig The Data to DataLake (Google Cloud Storage), Preparing the Data, Moving The Data to DataWarehouse (BigQuery), Transforming the Data to Create Aggregation Layers and RFM Analysis to Segment the Customers.

## Objective:

Transforming The raw transactional data that comes from postgres database on a daily basis to aggreagated layers and analytical layers that could be used to segment and analyse product success and targeting customers.

for more information about data source, check <a href="fake-data-generator/README.md">Data Source README</a>

for more information about final models and production tables Structure, check <a href="sql/README.md">final tables README</a>

## What Technologies are being Used?

- Cloud: [Google Cloud](https://cloud.google.com)
- Infrastructure: [Terraform](https://www.terraform.io/)
- Orchestration: [Prefect](https://www.prefect.io/)
- Data lake: [Google Cloud Storage](https://cloud.google.com/storage)
- Data warehouse: [BigQuery](https://cloud.google.com/bigquery)
- Data visualization: [Google Looker Studio](https://cloud.google.com/looker)

## Final Dashboard:

visit [Dashboard-Link](https://lookerstudio.google.com/s/hEWMeceunA8)

## How to Make it Work?

1. Setup your Google Cloud environment
  - Create a [Google Cloud Platform project](https://console.cloud.google.com/cloud-resource-manager)
  - Configure Identity and Access Management (IAM) for the service account, giving it the following privileges: BigQuery Admin, Storage Admin and Storage Object Admin
  - Download the JSON credentials and save it, e.g. to `~/.gc/<credentials>`
  - Install the [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
  - Let the [environment variable point to your GCP key](https://cloud.google.com/docs/authentication/application-default-credentials#GAC), authenticate it and refresh the session token
```bash
export GOOGLE_APPLICATION_CREDENTIALS=<path_to_your_credentials>.json
gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
gcloud auth application-default login
```
2. Setup your infrastructure
  - Assuming you are using Linux AMD64 run the following commands to install Terraform - if you are using a different OS please choose the correct version [here](https://developer.hashicorp.com/terraform/downloads) and exchange the download link and zip file name
```bash
sudo apt-get install unzip
cd ~/bin
wget https://releases.hashicorp.com/terraform/1.4.1/terraform_1.4.1_linux_amd64.zip
unzip terraform_1.4.1_linux_amd64.zip
rm terraform_1.4.1_linux_amd64.zip
```
3. To initiate, plan and apply the infrastructure, adjust and run the following Terraform commands
```bash
cd terraform/
terraform init
terraform plan -var="project=<your-gcp-project-id>"
terraform apply -var="project=<your-gcp-project-id>"
```
4. Go to fake-data-generator Directory and run the following to commands
  - run `make run_img` create and run the postgres docker image(Source System).
  - run `make run` create data and insert it to postgres.

5. Setup your Orchestration
  - Go to prefect Directory
  - to setup the python virtual environment and install all dependancies run `make venv`
  - check <a href = "prefect/README.md">prefect README</a> to setup the blocks and dependancies before running the flow.
  - run the flow using `make run`
6. the final tables will be created at online_store_data dataset in [BigQuery](https://cloud.google.com/bigquery).
7. build the dashboard.