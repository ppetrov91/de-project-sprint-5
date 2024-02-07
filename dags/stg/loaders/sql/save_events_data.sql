INSERT INTO stg.bonussystem_events
VALUES (%s, %s, %s, %s)
    ON CONFLICT (id)
    DO UPDATE
	  SET event_ts = EXCLUDED.event_ts
	    , event_type = EXCLUDED.event_type
	    , event_value = EXCLUDED.event_value;
