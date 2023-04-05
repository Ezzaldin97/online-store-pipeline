CREATE OR REPLACE PROCEDURE online_store_data.p_rfm_analysis() 
BEGIN
  DECLARE processing_date DATE;
  SET processing_date = CURRENT_DATE();
  DELETE FROM `online_store_data.RFM_analysis` WHERE profile_date < processing_date - 30;
  INSERT INTO `online_store_data.RFM_analysis`(profile_date, customer_id, monetary, frequency, recency, m_q, f_q, r_q)
  SELECT processing_date,
         customer_id,
         monetary,
         frequency,
         recency,
         ntile(5) OVER (ORDER BY monetary DESC) AS q_m,
         ntile(5) OVER (ORDER BY frequency DESC) AS q_f,
         ntile(5) OVER (ORDER BY recency) AS q_r
  FROM(
    SELECT customer_id,
           DATE_DIFF(processing_date, created_at, DAY) AS recency,
           SUM(cnt_orders) AS frequency,
           SUM(revenue_after_discount) AS monetary
    FROM `online_store_data.agg_orders_fact`
    WHERE profile_date >= CURRENT_DATE() - 30
    GROUP BY 1, 2
  ) AS t;
END;