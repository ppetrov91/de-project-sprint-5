INSERT INTO stg.bonussystem_ranks
VALUES(%s, %s, %s, %s)
    ON CONFLICT(id)
    DO UPDATE
          SET name = EXCLUDED.name
            , bonus_percent = EXCLUDED.bonus_percent
            , min_payment_threshold = EXCLUDED.min_payment_threshold;
