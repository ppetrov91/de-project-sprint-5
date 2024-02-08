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

CREATE OR REPLACE PROCEDURE dds.fill_dm_users()
AS
$$
DECLARE
  v_last_update_ts timestamp;
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

  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v.workflow_key
       , jsonb_build_object(v.search_key, v.update_ts::text) AS data
    FROM (SELECT u.update_ts
  	       , v_workflow_key AS workflow_key
  	       , v_key AS search_key
            FROM stg.ordersystem_users u
           WHERE u.update_ts > v_last_update_ts
           ORDER BY u.update_ts DESC
           LIMIT 1
         ) v
      ON CONFLICT(workflow_key)
      DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings;

  ANALYZE dds.dm_users;
  ANALYZE dds.srv_wf_settings;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_restaurants()
AS
$$
DECLARE
  v_last_update_ts timestamp;
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
       , MAX(r.update_ts) OVER() AS max_update_ts
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
   ORDER BY d.restaurant_id
  ),
  ins_new_rest_ver AS (
  /* Insert new version of already existing restaurant */
  INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
  SELECT d.restaurant_id
       , d.restaurant_name
       , d.active_from
       , d.active_to
    FROM ds d
    JOIN upd_rest u
      ON d.restaurant_id = u.restaurant_id
   ORDER BY d.restaurant_id
  )
  /* Write update_ts to rv_wf_settings */
  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v.*
    FROM (SELECT v_workflow_key
               , jsonb_build_object(v_key, d.max_update_ts::text) AS data
            FROM ds d
           LIMIT 1
         ) v
      ON CONFLICT(workflow_key)
      DO UPDATE
            SET workflow_settings = EXCLUDED.workflow_settings
          WHERE srv_wf_settings.workflow_settings != EXCLUDED.workflow_settings;

  ANALYZE dds.dm_restaurants;
  ANALYZE dds.srv_wf_settings;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_couriers()
AS
$$
DECLARE
  v_last_update_ts timestamp;
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

  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v.workflow_key
       , jsonb_build_object(v.search_key, v.update_ts) AS data
    FROM (SELECT date_trunc('seconds', c.update_ts)::text AS update_ts
               , v_workflow_key AS workflow_key
               , v_key AS search_key
            FROM stg.deliverysystem_couriers c
           WHERE c.update_ts > v_last_update_ts
           ORDER BY c.update_ts DESC
           LIMIT 1
         ) v
      ON CONFLICT(workflow_key)
      DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings;

  ANALYZE dds.dm_couriers;
  ANALYZE dds.srv_wf_settings;
END
$$
LANGUAGE plpgsql;

CREATE OR REPLACE PROCEDURE dds.fill_dm_timestamps()
AS
$$
DECLARE
  v_last_update_ts timestamp;
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

  INSERT INTO dds.srv_wf_settings(workflow_key, workflow_settings)
  SELECT v.workflow_key
       , jsonb_build_object(v.search_key, v.update_ts::text) AS data
    FROM (SELECT o.update_ts
  	       , v_workflow_key AS workflow_key
  	       , v_key AS search_key
            FROM stg.ordersystem_orders o
           WHERE o.update_ts > v_last_update_ts
           ORDER BY o.update_ts DESC
           LIMIT 1
         ) v
      ON CONFLICT(workflow_key)
      DO UPDATE SET workflow_settings = EXCLUDED.workflow_settings;

  ANALYZE dds.dm_timestamps;
  ANALYZE dds.srv_wf_settings;
END
$$
LANGUAGE plpgsql;
