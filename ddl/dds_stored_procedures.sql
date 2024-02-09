CREATE SCHEMA IF NOT EXISTS dds;

CREATE OR REPLACE FUNCTION dds.get_last_processed_val(p_schema text,
	 					      p_workflow_key text, 
						      p_key text,
						      p_def_val text) RETURNS text
AS
$$
DECLARE
  v_res text;
  v_query_tmpl text := 'SELECT COALESCE(s.workflow_settings->>$1, v.def_val) AS res
  			  FROM (SELECT $2 AS def_val) v
			  LEFT JOIN %1$s.srv_wf_settings s
			    ON s.workflow_key = $3';
BEGIN
  EXECUTE FORMAT(v_query_tmpl, p_schema) INTO v_res USING p_key, p_def_val, p_workflow_key;
  RETURN v_res;
END
$$
LANGUAGE plpgsql STABLE;

CREATE OR REPLACE PROCEDURE dds.fill_dm_users()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'dm_users';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */	
  SELECT u.object_value->>'_id' AS user_id
       , u.object_value->>'name' AS user_name
       , u.object_value->>'login' AS user_login
       , (MAX(u.update_ts) OVER())::text AS max_update_ts
    FROM stg.ordersystem_users u
   WHERE u.update_ts >= v_last_update_ts
  ),
  data AS (
  /* We don't excected user to be changed */	
  INSERT INTO dds.dm_users(user_id, user_name, user_login)
  SELECT d.user_id
       , d.user_name
       , d.user_login
    FROM ds d
      ON CONFLICT(user_id) DO NOTHING
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE 
            SET workflow_settings = EXCLUDED.workflow_settings
          WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;
      
  ANALYZE dds.srv_wf_settings;
  
  ANALYZE dds.dm_users;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_couriers()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'dm_couriers';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;
  
  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT c.object_id AS courier_id
       , regexp_split_to_array(c.object_value->>'name', ' ') AS courier_name_surname
       , (date_trunc('seconds', MAX(c.update_ts) OVER()))::text AS max_update_ts
    FROM stg.deliverysystem_couriers c
   WHERE c.update_ts >= v_last_update_ts
  ),
  data AS (
  /* For couriers SCD1 was chosen */
  INSERT INTO dds.dm_couriers(courier_id, courier_name, courier_surname)
  SELECT v.courier_id
       , v.courier_name_surname[1] AS courier_name
       , v.courier_name_surname[2] AS courier_surname
    FROM ds v
      ON CONFLICT(courier_id)
      DO UPDATE
            SET courier_name = EXCLUDED.courier_name
              , courier_surname = EXCLUDED.courier_surname
          WHERE dm_couriers.courier_name != EXCLUDED.courier_name
             OR dm_couriers.courier_surname != EXCLUDED.courier_surname
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;

  ANALYZE dds.dm_couriers;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_addresses()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'dm_addresses';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT regexp_split_to_array(d.object_value->>'address', ',') AS addr
       , (date_trunc('seconds', MAX(d.update_ts) OVER()))::text AS max_update_ts
    FROM stg.deliverysystem_deliveries d
   WHERE d.update_ts >= v_last_update_ts
  ),
  data AS (
  /* We don't expect dm_addresses to be changed */	
  INSERT INTO dds.dm_addresses(street_name, house_num, flat_num)
  SELECT DISTINCT v.addr[1] AS street_name
       , v.addr[2]::smallint AS house_num
       , substring(v.addr[3] from 5)::smallint AS flat_num
    FROM ds v
      ON CONFLICT(street_name, house_num, flat_num) DO NOTHING
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE 
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;
  
  ANALYZE dds.dm_addresses;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_timestamps()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'dm_timestamps';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT DISTINCT v.ts
       , v.max_update_ts
    FROM (SELECT (o.object_value->>'date')::timestamp AS ts
               , (MAX(o.update_ts) OVER())::text AS max_update_ts
            FROM stg.ordersystem_orders o
           WHERE o.update_ts >= v_last_update_ts
         ) v
  ),
  data AS (
  /* We don't expect timestamps to be changed */
  INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
  SELECT v.ts
       , EXTRACT(year FROM v.ts) AS year
       , EXTRACT(month FROM v.ts) AS month
       , EXTRACT(day FROM v.ts) AS day
       , ts::time AS time
       , ts::date AS date
    FROM ds v
      ON CONFLICT (ts) DO NOTHING
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;

  ANALYZE dds.dm_timestamps;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_restaurants()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'dm_restaurants';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT r.object_id AS restaurant_id
       , r.object_value->>'name' AS restaurant_name
       , r.update_ts AS active_from
       , '2099-12-31'::timestamp AS active_to
       , v_def_val::timestamp AS first_date
       , (MAX(r.update_ts) OVER())::text AS max_update_ts
    FROM stg.ordersystem_restaurants r
   WHERE r.update_ts >= v_last_update_ts
  ),
  upd_rest AS (
  /*Update current version if its name differs from name which was gathered by orders system*/	
  UPDATE dds.dm_restaurants r
     SET active_to = d.active_from
    FROM ds d
   WHERE r.active_to = '2099-12-31'::timestamp
     AND r.restaurant_id = d.restaurant_id
     AND r.restaurant_name != d.restaurant_name
  RETURNING r.restaurant_id
  ),
  upd_rest_cnt AS (
  SELECT COUNT(1)
    FROM upd_rest
  ),
  ins_new_rest AS (
  /* When we insert first version of a restaurant set active_to with 2022-01-01. 

     It is vital since during joins we need to get the right version of the row.

     In this case we use order_date to find the suitable version of the restaurants 
  */
  INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
  SELECT d.restaurant_id
       , d.restaurant_name
       , d.first_date AS active_from
       , d.active_to
    FROM ds d
    JOIN upd_rest_cnt u
      ON (1 = 1)
   WHERE NOT EXISTS (SELECT 1
                       FROM dds.dm_restaurants r
                      WHERE r.restaurant_id = d.restaurant_id
                    )
  ), 
  data AS (
  /* Insert new version of already existing restaurant */
  INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
  SELECT d.restaurant_id
       , d.restaurant_name
       , d.active_from
       , d.active_to
    FROM ds d
    JOIN upd_rest u
      ON d.restaurant_id = u.restaurant_id
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;

  ANALYZE dds.dm_restaurants;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_products()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'dm_products';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT r.id AS restaurant_id
       , v.menu_item->>'_id' AS product_id
       , v.menu_item->>'name' AS product_name
       , (v.menu_item->>'price')::numeric AS product_price
       , v.update_ts AS active_from
       , '2099-12-31'::timestamp AS active_to
       , '2022-01-01'::timestamp AS first_date
       , v.max_update_ts
    FROM (SELECT r.object_id AS restaurant_id
               , r.object_value->>'name' AS restaurant_name
               , jsonb_array_elements(r.object_value->'menu') AS menu_item
               , r.update_ts
               , (MAX(r.update_ts) OVER())::text AS max_update_ts
            FROM stg.ordersystem_restaurants r
           WHERE r.update_ts >= v_last_update_ts
         ) v
    JOIN dds.dm_restaurants r
      ON r.restaurant_id = v.restaurant_id
     AND v.update_ts BETWEEN r.active_from AND r.active_to
  ),
  upd_prods AS (
  /* Update current version of a restaurant if restaurant_id, product_name or product_price has been changed */
  UPDATE dds.dm_products p
     SET active_to = d.active_from
    FROM ds d
   WHERE p.product_id = d.product_id
     AND p.active_to = '2099-12-31'
     AND (p.restaurant_id != d.restaurant_id OR
          p.product_name != d.product_name OR
          p.product_price != p.product_price)
  RETURNING p.product_id
  ),
  upc AS (
  SELECT COUNT(1) AS cnt
    FROM upd_prods u
  ),
  ins_new_prods AS (
  /* When we insert first version of a product set active_to with 2022-01-01. 

     It is vital since during joins we need to get the right version of the row.

     In this case we use order_date to find the suitable version of the product
  */
  INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
  SELECT d.restaurant_id
       , d.product_id
       , d.product_name
       , d.product_price
       , d.first_date
       , d.active_to
    FROM ds d
    JOIN upc u
      ON (1 = 1)
   WHERE NOT EXISTS (SELECT 1
                       FROM dds.dm_products p
                      WHERE p.product_id = d.product_id
                    )
  ),
  data AS (
  /* Insert new version of already existing restaurant */
  INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
  SELECT d.restaurant_id
       , d.product_id
       , d.product_name
       , d.product_price
       , d.active_from
       , d.active_to
    FROM ds d
    JOIN upd_prods u
      ON d.product_id = u.product_id
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE 
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;

  ANALYZE dds.dm_products;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_deliveries()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'dm_deliveries';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT c.id AS courier_id
       , d.object_value->>'delivery_id' AS delivery_id
       , date_trunc('seconds', (d.object_value->>'delivery_ts')::timestamp) AS delivery_date
       , regexp_split_to_array(d.object_value->>'address', ',') AS addr
       , (date_trunc('seconds', MAX(d.update_ts) OVER()))::text AS max_update_ts
    FROM stg.deliverysystem_deliveries d
    JOIN dds.dm_couriers c
      ON c.courier_id = d.object_value->>'courier_id'
   WHERE d.update_ts >= v_last_update_ts
  ),
  data AS (
  /* Use SCD1 for dm_addresses */
  INSERT INTO dds.dm_deliveries(courier_id, address_id, delivery_date, delivery_id)
  SELECT v.courier_id
       , a.id AS address_id
       , v.delivery_date
       , v.delivery_id
    FROM ds v
    JOIN dds.dm_addresses a
      ON a.street_name = v.addr[1]
     AND a.house_num = v.addr[2]::smallint
     AND a.flat_num = substring(v.addr[3] from 5)::smallint
      ON CONFLICT(delivery_id)
      DO UPDATE
	    SET courier_id = EXCLUDED.courier_id
	      , address_id = EXCLUDED.address_id
	      , delivery_date = EXCLUDED.delivery_date
	  WHERE dm_deliveries.courier_id != EXCLUDED.courier_id
	     OR dm_deliveries.address_id != EXCLUDED.address_id
	     OR dm_deliveries.delivery_date != EXCLUDED.delivery_date
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE 
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;

  ANALYZE dds.dm_deliveries;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_orders()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'dm_orders';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT u.id AS user_id
       , re.id AS restaurant_id
       , t.id AS timestamp_id
       , r.object_id AS order_key
       , r.object_value->>'final_status' AS order_status
       , (MAX(r.update_ts) OVER())::text AS max_update_ts
    FROM stg.ordersystem_orders r
    JOIN dds.dm_users u
      ON u.user_id = r.object_value->'user'->>'id'
    JOIN dds.dm_timestamps t
      ON t.ts = (r.object_value->>'date')::timestamp
    JOIN dds.dm_restaurants re
      ON re.restaurant_id = r.object_value->'restaurant'->>'id'
     AND (r.object_value->>'date')::timestamp BETWEEN re.active_from AND re.active_to
   WHERE r.update_ts >= v_last_update_ts
  ),
  data AS (
  INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status)
  SELECT d.user_id
       , d.restaurant_id
       , d.timestamp_id
       , d.order_key
       , d.order_status
    FROM ds d
      ON CONFLICT(order_key)
      DO UPDATE
            SET user_id = EXCLUDED.user_id
              , restaurant_id = EXCLUDED.restaurant_id
              , timestamp_id = EXCLUDED.timestamp_id
              , order_key = EXCLUDED.order_key
              , order_status = EXCLUDED.order_status
	  WHERE dm_orders.user_id != EXCLUDED.user_id OR
	        dm_orders.restaurant_id != EXCLUDED.restaurant_id OR
		dm_orders.timestamp_id != EXCLUDED.timestamp_id OR
		dm_orders.order_key != EXCLUDED.order_key
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE 
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;

  ANALYZE dds.dm_orders;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_fct_order_deliveries()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'fct_order_deliveries';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT dl.id AS delivery_id
       , o.id AS order_id
       , date_trunc('seconds', (d.object_value->>'delivery_ts')::timestamp) AS delivery_date
       , (d.object_value->>'rate')::smallint AS rate
       , (d.object_value->>'tip_sum')::numeric AS tip_sum
       , (d.object_value->>'sum')::numeric AS order_total_sum
       , (date_trunc('seconds', MAX(d.update_ts) OVER()))::text AS max_update_ts
    FROM stg.deliverysystem_deliveries d
    JOIN dds.dm_deliveries dl
      ON dl.delivery_id = d.object_value->>'delivery_id'
    JOIN dds.dm_orders o
      ON o.order_key = d.object_value->>'order_id'
   WHERE d.update_ts >= v_last_update_ts
  ),
  data AS (
  INSERT INTO dds.fct_order_deliveries(delivery_id, order_id, delivery_date, rate, order_total_sum, tip_sum)
  SELECT d.delivery_id, d.order_id, d.delivery_date, d.rate, d.order_total_sum, d.tip_sum
    FROM ds d
      ON CONFLICT(delivery_id, order_id)
      DO UPDATE
            SET delivery_date = EXCLUDED.delivery_date
              , rate = EXCLUDED.rate
              , tip_sum = EXCLUDED.tip_sum
          WHERE fct_order_deliveries.delivery_date != EXCLUDED.delivery_date
             OR fct_order_deliveries.rate != EXCLUDED.rate
             OR fct_order_deliveries.tip_sum != EXCLUDED.tip_sum
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts)
    FROM ds d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;

  ANALYZE dds.fct_order_deliveries;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_fct_product_sales()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_workflow_key text := 'fct_product_sales';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  /* Grab last processed ts from dds.srv_wf_settings */
  v_last_update_ts = dds.get_last_processed_val(v_wf_settings_schema, v_workflow_key, v_key, v_def_val)::timestamp;

  WITH orders AS (
  /* Grab records which update_ts is greater or equal then v_last_update_ts */
  SELECT p.id AS product_id
       , v.order_id
       , (v.order_item->>'quantity')::int AS product_count
       , (v.order_item->>'price')::numeric AS product_price
       , v.max_update_ts
    FROM (SELECT o.id AS order_id
               , so.object_value
               , so.object_id AS order_id_ext
               , (so.object_value->>'date')::timestamp AS order_date
               , so.object_value->'restaurant'->>'id' AS restaurant_id_ext
               , jsonb_array_elements(so.object_value->'order_items') AS order_item
               , (MAX(so.update_ts) OVER())::text AS max_update_ts
            FROM stg.ordersystem_orders so
            JOIN dds.dm_orders o
              ON o.order_key = so.object_id
           WHERE so.update_ts >= v_last_update_ts
             AND so.object_value->>'final_status' = 'CLOSED'
          ) v
    JOIN dds.dm_products p
      ON p.product_id = v.order_item->>'id'
     AND v.order_date BETWEEN p.active_from AND p.active_to
  ),
  bonus AS (
  SELECT p.id AS product_id
       , o.id AS order_id
       , (v.prod_payment->>'bonus_grant')::numeric AS bonus_grant
       , (v.prod_payment->>'bonus_payment')::numeric AS bonus_payment
    FROM (SELECT e.event_value->>'order_id' AS order_key
               , (e.event_value->>'order_date')::timestamp AS order_date
               , jsonb_array_elements(e.event_value->'product_payments') AS prod_payment
            FROM stg.bonussystem_events e
           WHERE e.event_type = 'bonus_transaction'
             AND e.event_ts >= v_last_update_ts
         ) v
    JOIN dds.dm_products p
      ON p.product_id = v.prod_payment->>'product_id'
     AND v.order_date BETWEEN p.active_from AND p.active_to
    JOIN dds.dm_orders o
      ON o.order_key = v.order_key
  ),
  data AS (
  INSERT INTO dds.fct_product_sales(product_id, order_id, product_count, product_price, total_sum, bonus_payment, bonus_grant)
  SELECT o.product_id
       , o.order_id
       , o.product_count
       , o.product_price
       , o.product_count * o.product_price AS total_sum
       , COALESCE(b.bonus_grant, 0) AS bonus_grant
       , COALESCE(b.bonus_payment, 0) AS bonus_payment
    FROM orders o
    LEFT JOIN bonus b
      ON b.order_id = o.order_id
     AND b.product_id = o.product_id
      ON CONFLICT(product_id, order_id)
      DO UPDATE
            SET product_count = EXCLUDED.product_count
              , product_price = EXCLUDED.product_price
              , total_sum = EXCLUDED.total_sum
              , bonus_payment = EXCLUDED.bonus_payment
              , bonus_grant = EXCLUDED.bonus_grant
	  WHERE fct_product_sales.product_count != EXCLUDED.product_count 
	     OR fct_product_sales.product_price != EXCLUDED.product_price
	     OR fct_product_sales.total_sum != EXCLUDED.total_sum
	     OR fct_product_sales.bonus_payment != EXCLUDED.bonus_payment
	     OR fct_product_sales.bonus_grant != EXCLUDED.bonus_grant
  )
  /* Grab the latest update_ts from new records and save it into dds.srv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v_workflow_key
       , jsonb_build_object(v_key, d.max_update_ts) 
    FROM orders d
   LIMIT 1
      ON CONFLICT(workflow_key)
      DO UPDATE
	    SET workflow_settings = EXCLUDED.workflow_settings
	  WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.srv_wf_settings;

  ANALYZE dds.fct_product_sales;
END
$$
LANGUAGE plpgsql;
