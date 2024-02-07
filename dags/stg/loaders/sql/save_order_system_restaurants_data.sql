INSERT INTO stg.ordersystem_restaurants(object_id, object_value, update_ts)
VALUES (%s, %s, %s)
    ON CONFLICT (object_id)
    DO UPDATE
	  SET object_value = EXCLUDED.object_value
	    , update_ts = EXCLUDED.update_ts;
