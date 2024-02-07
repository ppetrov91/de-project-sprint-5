INSERT INTO stg.bonussystem_users VALUES(%s, %s)
    ON CONFLICT(id) 
    DO UPDATE
          SET order_user_id = EXCLUDED.order_user_id;
