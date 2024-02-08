CREATE SCHEMA IF NOT EXISTS dds;

CREATE OR REPLACE FUNCTION dds.get_last_processed_val(p_workflow_key text, 
						      p_key text,
						      p_def_val text) 
RETURNS text
AS
$$
SELECT COALESCE(s.workflow_settings->>p_key, v.def_val) AS res
  FROM (SELECT p_def_val AS def_val) v
  LEFT JOIN dds.srv_wf_settings s
    ON s.workflow_key = p_workflow_key;
$$
LANGUAGE sql STABLE;

CREATE OR REPLACE PROCEDURE dds.update_srv_wf_settings(p_wf_settings_schema text,
						       p_source_table_schema text,
						       p_source_table_name text,
						       p_last_update_ts timestamp,
						       p_workflow_key text,
						       p_key text
						      )
AS
$$
DECLARE
  v_query_tmpl text := 'INSERT INTO %1$s.srv_wf_settings(workflow_key, workflow_settings)
			SELECT v.workflow_key
			     , jsonb_build_object(v.search_key, v.update_ts) AS data
			  FROM (SELECT date_trunc(%2$L, c.update_ts)::text AS update_ts
				     , $2 AS workflow_key
				     , $3 AS search_key
			          FROM %3$s.%4$s c
				 WHERE c.update_ts > $1
				 ORDER BY c.update_ts DESC
				 LIMIT 1
			       ) v
			    ON CONFLICT(workflow_key)
			    DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings';
BEGIN
  EXECUTE FORMAT(v_query_tmpl, p_wf_settings_schema, 'seconds', p_source_table_schema, p_source_table_name) 
    USING p_last_update_ts, p_workflow_key, p_key;

  EXECUTE FORMAT('ANALYZE %1$s.srv_wf_settings', p_wf_settings_schema);
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_users()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_source_table_schema text := 'stg';
  v_source_table_name text := 'ordersystem_users';
  v_workflow_key text := 'dm_users';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_update_ts = dds.get_last_processed_val(v_workflow_key, v_key, v_def_val)::timestamp;

  INSERT INTO dds.dm_users(user_id, user_name, user_login)
  SELECT u.object_value->>'_id' AS user_id
       , u.object_value->>'name' AS user_name
       , u.object_value->>'login' AS user_login
    FROM stg.ordersystem_users u
   WHERE u.update_ts > v_last_update_ts
      ON CONFLICT(user_id) DO NOTHING;

  CALL dds.update_srv_wf_settings(v_wf_settings_schema, v_source_table_schema, v_source_table_name, 
				  v_last_update_ts, v_workflow_key, v_key);

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
  v_source_table_schema text := 'stg';
  v_source_table_name text := 'deliverysystem_couriers';
  v_workflow_key text := 'dm_couriers';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_update_ts = dds.get_last_processed_val(v_workflow_key, v_key, v_def_val)::timestamp;

  INSERT INTO dds.dm_couriers(courier_id, courier_name, courier_surname)
  SELECT v.courier_id
       , v.courier_name_surname[1] AS courier_name
       , v.courier_name_surname[2] AS courier_surname
    FROM (SELECT c.object_id AS courier_id
               , regexp_split_to_array(c.object_value->>'name', ' ') AS courier_name_surname
            FROM stg.deliverysystem_couriers c
           WHERE c.update_ts > v_last_update_ts
         ) v
      ON CONFLICT(courier_id)
      DO UPDATE
            SET courier_name = EXCLUDED.courier_name
              , courier_surname = EXCLUDED.courier_surname
          WHERE dm_couriers.courier_name != EXCLUDED.courier_name
             OR dm_couriers.courier_surname != EXCLUDED.courier_surname;

  CALL dds.update_srv_wf_settings(v_wf_settings_schema, v_source_table_schema, v_source_table_name,
                                  v_last_update_ts, v_workflow_key, v_key);

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
  v_source_table_schema text := 'stg';
  v_source_table_name text := 'deliverysystem_deliveries';
  v_workflow_key text := 'dm_addresses';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_update_ts = dds.get_last_processed_val(v_workflow_key, v_key, v_def_val)::timestamp;

  INSERT INTO dds.dm_addresses(street_name, house_num, flat_num)
  SELECT DISTINCT v.addr[1] AS street_name
       , v.addr[2]::smallint AS house_num
       , substring(v.addr[3] from 5)::smallint AS flat_num
    FROM (SELECT regexp_split_to_array(d.object_value->>'address', ',') AS addr
            FROM stg.deliverysystem_deliveries d
           WHERE d.update_ts > v_last_update_ts
         ) v
      ON CONFLICT(street_name, house_num, flat_num) DO NOTHING;

  CALL dds.update_srv_wf_settings(v_wf_settings_schema, v_source_table_schema, v_source_table_name,
                                  v_last_update_ts, v_workflow_key, v_key);

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
  v_source_table_schema text := 'stg';
  v_source_table_name text := 'ordersystem_orders';
  v_workflow_key text := 'dm_timestamps';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_update_ts = dds.get_last_processed_val(v_workflow_key, v_key, v_def_val)::timestamp;

  INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
  SELECT v.ts
       , EXTRACT(year FROM v.ts) AS year
       , EXTRACT(month FROM v.ts) AS month
       , EXTRACT(day FROM v.ts) AS day
       , ts::time AS time
       , ts::date AS date
    FROM (SELECT DISTINCT (o.object_value->>'date')::timestamp AS ts
            FROM stg.ordersystem_orders o
           WHERE o.update_ts > v_last_update_ts
         ) v
      ON CONFLICT (ts) DO NOTHING;

  CALL dds.update_srv_wf_settings(v_wf_settings_schema, v_source_table_schema, v_source_table_name,
                                  v_last_update_ts, v_workflow_key, v_key);

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
  v_source_table_schema text := 'stg';
  v_source_table_name text := 'ordersystem_restaurants';
  v_workflow_key text := 'dm_restaurants';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_update_ts = dds.get_last_processed_val(v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  SELECT r.object_id AS restaurant_id
       , r.object_value->>'name' AS restaurant_name
       , r.update_ts AS active_from
       , '2099-12-31'::timestamp AS active_to
       , v_def_val::timestamp AS first_date
    FROM stg.ordersystem_restaurants r
   WHERE r.update_ts > v_last_update_ts
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
  /* When we insert first version of a restaurant fill active_to with 2022-01-01. 

     It is vital since during joins we need to get the right version of the row.

     For example, we use order_date to find the suitable version of the restaurants 
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
  )
  /* Insert new version of already existing restaurant */
  INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
  SELECT d.restaurant_id
       , d.restaurant_name
       , d.active_from
       , d.active_to
    FROM ds d
    JOIN upd_rest u
      ON d.restaurant_id = u.restaurant_id;

  CALL dds.update_srv_wf_settings(v_wf_settings_schema, v_source_table_schema, v_source_table_name,
                                  v_last_update_ts, v_workflow_key, v_key);

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
  v_source_table_schema text := 'stg';
  v_source_table_name text := 'ordersystem_restaurants';
  v_workflow_key text := 'dm_products';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_update_ts = dds.get_last_processed_val(v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  SELECT r.id AS restaurant_id
       , v.menu_item->>'_id' AS product_id
       , v.menu_item->>'name' AS product_name
       , (v.menu_item->>'price')::numeric AS product_price
       , v.update_ts AS active_from
       , '2099-12-31'::timestamp AS active_to
       , '2022-01-01'::timestamp AS first_date
    FROM (SELECT r.object_id AS restaurant_id
               , r.object_value->>'name' AS restaurant_name
               , jsonb_array_elements(r.object_value->'menu') AS menu_item
               , r.update_ts
            FROM stg.ordersystem_restaurants r
           WHERE r.update_ts > v_last_update_ts
         ) v
    JOIN dds.dm_restaurants r
      ON r.restaurant_id = v.restaurant_id
     AND v.update_ts BETWEEN r.active_from AND r.active_to
  ),
  upd_prods AS (
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
  )
  INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
  SELECT d.restaurant_id
       , d.product_id
       , d.product_name
       , d.product_price
       , d.active_from
       , d.active_to
    FROM ds d
    JOIN upd_prods u
      ON d.product_id = u.product_id;

  CALL dds.update_srv_wf_settings(v_wf_settings_schema, v_source_table_schema, v_source_table_name,
                                  v_last_update_ts, v_workflow_key, v_key);

  ANALYZE dds.dm_products;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_orders()
AS
$$
DECLARE
  v_last_update_ts timestamp;
  v_wf_settings_schema text := 'dds';
  v_source_table_schema text := 'stg';
  v_source_table_name text := 'ordersystem_orders';
  v_workflow_key text := 'dm_orders';
  v_key text := 'last_update_ts';
  v_def_val text := '2022-01-01';
BEGIN
  v_last_update_ts = dds.get_last_processed_val(v_workflow_key, v_key, v_def_val)::timestamp;

  WITH ds AS (
  SELECT u.id AS user_id
       , re.id AS restaurant_id
       , t.id AS timestamp_id
       , r.object_id AS order_key
       , r.object_value->>'final_status' AS order_status
       , (r.object_value->>'date')::timestamp AS order_date
       , re.active_from
       , re.active_to
    FROM stg.ordersystem_orders r
    JOIN dds.dm_users u
      ON u.user_id = r.object_value->'user'->>'id'
    JOIN dds.dm_timestamps t
      ON t.ts = (r.object_value->>'date')::timestamp
    JOIN dds.dm_restaurants re
      ON re.restaurant_id = r.object_value->'restaurant'->>'id'
     AND (r.object_value->>'date')::timestamp BETWEEN re.active_from AND re.active_to
   WHERE r.update_ts > v_last_update_ts
  )
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
		dm_orders.order_key != EXCLUDED.order_key;

  CALL dds.update_srv_wf_settings(v_wf_settings_schema, v_source_table_schema, v_source_table_name,
                                  v_last_update_ts, v_workflow_key, v_key);
		
  ANALYZE dds.dm_orders;
END
$$
LANGUAGE plpgsql;
