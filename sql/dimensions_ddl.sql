CREATE OR REPLACE TABLE `online_store_data.items_dimension` (
  iid STRING NOT NULL,
  item STRING,
  price INT64,
  description STRING,
  discount_flag STRING,
  last_day_score FLOAT64
);

CREATE OR REPLACE TABLE `online_store_data.customer_profile` (
  id INT64 NOT NULL,
  country STRING,
  address STRING,
  gender STRING,
  credit_card_provider STRING,
  credit_card_number INT64,
  credit_card_expire STRING,
  credit_card_security_code INT64
);