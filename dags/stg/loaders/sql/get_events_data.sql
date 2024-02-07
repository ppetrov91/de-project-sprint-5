SELECT o.id
     , o.event_ts
     , o.event_type
     , o.event_value
  FROM public.outbox o
 WHERE o.id > %s
 ORDER BY o.id
 LIMIT %s;
