CREATE OR REPLACE TABLE `online_store_data.RFM_analysis`(
  profile_date DATE,
  customer_id INT64 NOT NULL,
  monetary FLOAT64,
  frequency INT64,
  recency INT64,
  m_q INT64,
  f_q INT64,
  r_q INT64
)