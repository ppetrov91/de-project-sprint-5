SELECT u.id
     , u.order_user_id
  FROM public.users u
 WHERE u.id > %s
 ORDER BY u.id
 LIMIT %s;
