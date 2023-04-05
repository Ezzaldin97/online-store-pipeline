CREATE OR REPLACE TABLE `online_store_data.agg_orders_fact`(
  customer_id INT64 NOT NULL,
  iid STRING NOT NULL,
  created_at DATE,
  cnt_orders INT64,
  revenue INT64,
  revenue_after_discount FLOAT64,
  profile_date DATE
) PARTITION BY (profile_date)
OPTIONS (require_partition_filter = TRUE);