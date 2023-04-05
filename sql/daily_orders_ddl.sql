CREATE OR REPLACE TABLE `online_store_data.daily_orders`(
  customer_id INT64 NOT NULL,
  iid STRING NOT NULL,
  created_at DATE,
  profile_date DATE
) PARTITION BY (profile_date)
OPTIONS (require_partition_filter = TRUE);