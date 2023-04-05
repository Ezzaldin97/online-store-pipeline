# Fake Data Generator

## Description:

Part of Any Data Engineering/Data Science Project is to start search for the right data needed to complete the project, of course there are several ways to get data like using API services, free datasets platforms, or collecting surveies etc..

another way to get data is by generating it, this may be perfect to complete data engineering projects while focusing on tech part, or testing the pipeline.

## Installation & Requirements:

the software technologies must be installed before running the project:

- Python >= 3.7
- Docker
- Linux Machine or Git Bash for Windows

## How it Works?

This Project fully automated to run using make commands, different make commands used link dependancies needed to run the code/test the code/run docker images/stop the docker images/create the python environment needed and clean up the project if not needed.

### follow the steps below to run the project:

- make sure that make command installed on your system
  - for linux users run the following ` apt-get update && apt-get install make `
  - for Window Git Bash Users, please follow the steps: https://gist.github.com/evanwill/0207876c3243bbb6863e65ec5dc3f058
- all data will be stored on Postgres Docker Image, need to run the image first ` make run_img `
- run the data generator and store the data also on postgres using ` make run `, this will create the python virtual environment and install the following packages:
```txt
pandas==1.3.5
protobuf==4.21.11
pyarrow==10.0.1
psycopg2-binary==2.9.5
sqlalchemy==1.4.46
Faker==18.3.1
PyYAML==6.0
pytest==7.2.2
```
and then run the generator.
- stop the docker image using ` make stop_img `
- after finishing, we can clean up the project and delete everything including the storage on your system of docker postgres image using ` make clean `

## Generated Data:

Three Datasets will be generated:

- Customers Data: Contain informations about Customers.
- Items Data: Contain informations about Items.
- Orders Data: Contain Transactional Data for Customers and Ordered Items.

### Customers Data:

| Column | Description |
|--------|-------------|
| cid | Customer Id |
| dob | Date Of Birth |
| country | Country of this Customer |
| address | Customer Address |
| gender | Customer Gender |
| credit_card_provider | Credit Card Service Provider |
| credit_card_number | Credit Card Serial Number |
| credit_card_expire | Credit Card Expiry Date |
| credit_card_security_code | security code |

### Orders Data:

| Column | Description |
|--------|-------------|
| cid | Customer Id |
| item | Ordered Item |
| created_at | Date of the order |

### Items Data:

| Column | Description |
|--------|-------------|
| item | Item Name |
| price | Item Price |
| description | what's this item? |
| discount_flag | if there is a discount or not |
| last_day_sales | Number of Sales Last Day |
| last_day_review_score | average review score of this item |

