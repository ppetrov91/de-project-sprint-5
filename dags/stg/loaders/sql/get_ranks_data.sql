SELECT r.id
     , r.name
     , r.bonus_percent
     , r.min_payment_threshold
  FROM public.ranks r
 WHERE r.id > %s
 ORDER BY r.id
 LIMIT %s;
