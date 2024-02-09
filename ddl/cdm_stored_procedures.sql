CREATE SCHEMA IF NOT EXISTS cdm;

CREATE OR REPLACE PROCEDURE cdm.fill_dm_settlement_report()
AS
$$
DECLARE
  v_last_ts timestamp;
  v_wf_settings_schema text := 'cdm';
  v_workflow_key text := 'dm_settlement_report';
  v_key text := 'last_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  SELECT o.restaurant_id
       , r.restaurant_name
       , t."date"
       , fps.order_id
       , fps.total_sum
       , fps.bonus_payment
       , fps.bonus_grant
       , TO_CHAR(MAX(t.ts) OVER(), 'YYYY-MM-DD') AS max_ts
    FROM dds.dm_restaurants r
    JOIN dds.dm_orders o
      ON r.id = o.restaurant_id
     AND o.order_status = 'CLOSED'
    JOIN dds.dm_timestamps t
      ON t.id = o.timestamp_id
     AND t.ts >= v_last_ts
    LEFT JOIN dds.fct_product_sales fps
      ON fps.order_id = o.id
  ),
  data AS (
  INSERT INTO cdm.dm_settlement_report(restaurant_id, restaurant_name,
                                       settlement_date, orders_count,
                                       orders_total_sum, orders_bonus_payment_sum,
                                       orders_bonus_granted_sum, order_processing_fee,
                                       restaurant_reward_sum
                                      )
  SELECT o.restaurant_id
       , MAX(r.restaurant_name) AS restaurant_name
       , t."date" AS settlement_date
       , COUNT(DISTINCT fps.order_id) AS orders_count
       , SUM(fps.total_sum) AS orders_total_sum
       , SUM(fps.bonus_payment) AS orders_bonus_payment_sum
       , SUM(fps.bonus_grant) AS orders_bonus_granted_sum
       , SUM(fps.total_sum) * 0.25 AS order_processing_fee
       , GREATEST(0.75 * SUM(fps.total_sum) - SUM(fps.bonus_payment), 0) AS restaurant_reward_sum
    FROM dds.dm_restaurants r
    JOIN dds.dm_orders o
      ON r.id = o.restaurant_id
     AND o.order_status = 'CLOSED'
    JOIN dds.dm_timestamps t
      ON t.id = o.timestamp_id
     AND t.ts >= v_last_ts
    LEFT JOIN dds.fct_product_sales fps
      ON fps.order_id = o.id
   GROUP BY o.restaurant_id, t."date"
      ON CONFLICT (restaurant_id, settlement_date)
      DO UPDATE
            SET orders_count = EXCLUDED.orders_count
              , orders_total_sum = EXCLUDED.orders_total_sum
              , orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum
              , orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum
              , order_processing_fee = EXCLUDED.order_processing_fee
              , restaurant_reward_sum = EXCLUDED.restaurant_reward_sum
   )
   INSERT INTO cdm.srv_wf_settings(workflow_key, workflow_settings)
   SELECT v_workflow_key
        , jsonb_build_object(v_key, d.max_ts) 
     FROM ds d
    LIMIT 1
       ON CONFLICT(workflow_key)
       DO UPDATE 
             SET workflow_settings = EXCLUDED.workflow_settings
           WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

   ANALYZE cdm.srv_wf_settings;

   ANALYZE cdm.dm_settlement_report;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE cdm.fill_dm_courier_ledger()
AS
$$
DECLARE
  v_last_ts timestamp;
  v_wf_settings_schema text := 'cdm';
  v_workflow_key text := 'dm_courier_ledger';
  v_key text := 'last_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;
  
  WITH ds AS (
  SELECT d.courier_id
       , EXTRACT(year FROM f.delivery_date) AS settlement_year
       , EXTRACT(month FROM f.delivery_date) AS settlement_month
       , COUNT(f.order_id) AS orders_count
       , SUM(f.order_total_sum) AS orders_total_sum
       , SUM(f.order_total_sum) * 0.25 AS order_processing_fee
       , MAX(v.rate_avg) AS rate_avg
       
       , SUM(CASE
               WHEN v.rate_avg < 4 
                 THEN GREATEST(0.05 * f.order_total_sum, 100)
               WHEN v.rate_avg >= 4 AND f.rate < 4.5
                 THEN GREATEST(0.07 * f.order_total_sum, 150)
               WHEN v.rate_avg >= 4.5 AND f.rate < 4.9
                 THEN GREATEST(0.08 * f.order_total_sum, 175)
               ELSE GREATEST(0.1 * f.order_total_sum, 200)  
             END) AS courier_order_sum
       , SUM(f.tip_sum) AS courier_tips_sum
       , MAX(d.delivery_date) AS max_delivery_date
    FROM dds.fct_order_deliveries f
    JOIN dds.dm_deliveries d
      ON d.id = f.delivery_id
     AND d.delivery_date >= v_last_ts
    JOIN (SELECT dd.courier_id
             , EXTRACT(year FROM fod.delivery_date) AS settlement_year
             , EXTRACT(month FROM fod.delivery_date) AS settlement_month 
             , AVG(fod.rate) AS rate_avg
          FROM dds.fct_order_deliveries fod
          JOIN dds.dm_deliveries dd
            ON dd.id = fod.delivery_id
           AND dd.delivery_date >= v_last_ts
         WHERE fod.delivery_date >= v_last_ts
         GROUP BY 1, 2, 3
        ) v
     ON v.courier_id = d.courier_id
    AND v.settlement_year = EXTRACT(year FROM f.delivery_date)
    AND v.settlement_month = EXTRACT(month FROM f.delivery_date) 
  WHERE f.delivery_date >= v_last_ts
  GROUP BY 1, 2, 3
 ),
 prep_res AS (
 SELECT p.courier_id
      , c.courier_surname || ' ' || c.courier_name AS courier_name
      , p.settlement_year
      , p.settlement_month
      , p.orders_count
      , p.orders_total_sum
      , p.rate_avg
      , p.order_processing_fee
      , p.courier_order_sum
      , p.courier_tips_sum
      , p.courier_order_sum + p.courier_tips_sum * 0.95 AS courier_reward_sum
   FROM ds p
   JOIN dds.dm_couriers c
     ON c.id = p.courier_id
 ),
 data AS (
 INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, 
                                   settlement_month, orders_count, orders_total_sum,
                                   rate_avg, order_processing_fee, courier_order_sum, 
                                   courier_tips_sum, courier_reward_sum)
 SELECT p.courier_id
      , p.courier_name
      , p.settlement_year
      , p.settlement_month
      , p.orders_count
      , p.orders_total_sum
      , p.rate_avg
      , p.order_processing_fee
      , p.courier_order_sum
      , p.courier_tips_sum
      , p.courier_reward_sum
   FROM prep_res p  
     ON CONFLICT(courier_id, settlement_year, settlement_month)
     DO UPDATE
           SET courier_name = EXCLUDED.courier_name
             , settlement_year = EXCLUDED.settlement_year
             , settlement_month = EXCLUDED.settlement_month
             , orders_count = EXCLUDED.orders_count
             , orders_total_sum = EXCLUDED.orders_total_sum
             , rate_avg = EXCLUDED.rate_avg
             , order_processing_fee = EXCLUDED.order_processing_fee
             , courier_order_sum = EXCLUDED.courier_order_sum
             , courier_tips_sum = EXCLUDED.courier_tips_sum
             , courier_reward_sum = EXCLUDED.courier_reward_sum
         WHERE dm_courier_ledger.courier_name != EXCLUDED.courier_name
            OR dm_courier_ledger.settlement_year != EXCLUDED.settlement_year
            OR dm_courier_ledger.settlement_month != EXCLUDED.settlement_month
            OR dm_courier_ledger.orders_count != EXCLUDED.orders_count
            OR dm_courier_ledger.orders_total_sum != EXCLUDED.orders_total_sum
            OR dm_courier_ledger.rate_avg != EXCLUDED.rate_avg
            OR dm_courier_ledger.order_processing_fee != EXCLUDED.order_processing_fee
            OR dm_courier_ledger.courier_order_sum != EXCLUDED.courier_order_sum
            OR dm_courier_ledger.courier_tips_sum != EXCLUDED.courier_tips_sum
            OR dm_courier_ledger.courier_reward_sum != EXCLUDED.courier_reward_sum
  )
  INSERT INTO cdm.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, TO_CHAR(date_trunc('month', MAX(d.max_delivery_date)), 'YYYY-MM-DD')) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE 
            SET workflow_settings = EXCLUDED.workflow_settings
          WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE cdm.srv_wf_settings;

  ANALYZE cdm.dm_courier_ledger;
END
$$
LANGUAGE plpgsql;
