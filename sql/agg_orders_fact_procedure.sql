CREATE OR REPLACE PROCEDURE online_store_data.p_agg_orders_fact() 
BEGIN
  DECLARE processing_date DATE;
  SET processing_date = CURRENT_DATE();
  --CHECKPOINT 1
  INSERT INTO `online_store_data.items_dimension`(iid, item, price, description, discount_flag, last_day_score)
  SELECT s.iid,
         s.item,
         s.price,
         s.description,
         s.discount_flag,
         s.last_day_score
  FROM `staging.items` s
  LEFT JOIN `online_store_data.items_dimension` d ON d.iid = s.iid
  WHERE d.iid IS NULL;
  --CHECKPOINT 2
  INSERT INTO `online_store_data.customer_profile`(id, country, address, gender, credit_card_provider, credit_card_number, credit_card_expire, credit_card_security_code)
  SELECT s.id,
         s.country,
         s.address,
         s.gender,
         s.credit_card_provider,
         s.credit_card_number,
         s.credit_card_expire,
         s.credit_card_security_code
  FROM `staging.customers` s
  LEFT JOIN `online_store_data.customer_profile` cp ON cp.id = s.id
  WHERE cp.id IS NULL;
  --CHECKPOINT 3
  DELETE FROM `online_store_data.daily_orders` WHERE profile_date < processing_date-90;
  INSERT INTO `online_store_data.daily_orders`(customer_id, iid, created_at, profile_date)
  SELECT customer_id,
         iid,
         CAST(created_at AS DATE) AS created_at,
         processing_date
  FROM `staging.orders`;
  --CHECKPOINT 4
  -- AGGREGATED ORDERS FACT TABLE..
  DELETE FROM `online_store_data.agg_orders_fact` WHERE profile_date < processing_date-180;
  INSERT INTO `online_store_data.agg_orders_fact`(customer_id, iid, created_at, cnt_orders, revenue, revenue_after_discount, profile_date)
  SELECT customer_id,
         agg_orders.iid,
         created_at,
         cnt_orders,
         cnt_orders * price,
         CASE
             WHEN discount_flag = 'N' THEN cnt_orders * price
             ELSE cnt_orders * price * 0.2
         END,
         processing_date
  FROM (
    SELECT customer_id,
           iid,
           created_at,
           COUNT(*) AS cnt_orders
    FROM `online_store_data.daily_orders`
    WHERE profile_date >= processing_date-4
    GROUP BY 1, 2, 3
  ) AS agg_orders
  LEFT JOIN `online_store_data.items_dimension` AS d ON d.iid = agg_orders.iid;
  DROP TABLE `staging.items`;
  DROP TABLE `staging.orders`;
  DROP TABLE `staging.customers`;
END;