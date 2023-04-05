# Stored Procedures and DDL Commands of Tables in BigQuery:

## Contents:

- dimensions_ddl: DDL Queries of Items Dimension Table, and Customer Profile Table.
- daily_orders_ddl: DDL Query of Daily Orders Table(Transactional Table of Customer, Item, Date of Order).
- agg_orders_fact_ddl: DDL Query of Aggregate Orders Fact Table that holds aggreagted data about orders.
- RFM_Analysis_ddl: DDL Query of RFM Analysis Table.
- p_agg_orders_fact: Stored Procedure Used to Fill dimension Tables, daily orders table and aggregated orders fact table.
- p_RFM_analysis: Stored Procedure Used to Create Simple RFM Analysis for each Customer.

## Stored Procedures Description:

### p_agg_orders_fact:

Stored Procedure Used to Update Items Dimension Table, Add New Customers to Customer Profile Table, Insert New Data to Daily Orders Table (Covers 90 Days), and Create aggregated orders fact table.

#### agg_orders_fact:

| Column | Description |
|--------|-------------|
| customer_id | unique identifier of customer |
| iid | unique identifier of items |
| created_at | date of the order |
| cnt_orders | count of orders |
| revenue | total revenue before discount |
| revenue_after_discount | total revenue after discount |
| profile_date | running date of stored procedure |

### p_RFM_Analysis:

Stored Procedure Used to Create Simple RFM Analysis for each Customer to Segment each Customer.

#### RFM_analysis:

| Column | Description |
|--------|-------------|
| profile_date | running date of stored procedure |
| customer_id | unique identifier of customer |
| monetary | total revenue |
| frequency | frequency of orders |
| recency | lowest recency |
| m_q | monetary quatile |
| m_f | frequency quatile |
| m_r | recency quatile |