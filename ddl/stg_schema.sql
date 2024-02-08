CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.bonussystem_users (
    id bigint CONSTRAINT bonussystem_users_pkey PRIMARY KEY,
    order_user_id text NOT NULL
); 

CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id bigint not null PRIMARY KEY,
    name varchar(2048) not null,
    bonus_percent numeric(19,5) not null default 0,
    min_payment_threshold numeric(19,5) not null default 0,
    CONSTRAINT bonussystem_ranks_bonus_percent_check CHECK (bonus_percent >= 0),
    CONSTRAINT bonussystem_ranks_min_payment_threshold_check CHECK (min_payment_threshold >= 0)
);

CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
    id bigint not null PRIMARY KEY,
    event_ts timestamp not null,
    event_type varchar not null,
    event_value jsonb not null
);

CREATE INDEX IF NOT EXISTS idx_outbox__event_ts 
    ON stg.bonussystem_events(event_ts);

CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
    id bigserial not null primary key,
    object_id varchar not null,
    object_value jsonb not null,
    update_ts timestamp not null,
    CONSTRAINT ordersystem_users_object_id_uindex UNIQUE(object_id)
);

CREATE INDEX IF NOT EXISTS ou_update_ts ON stg.ordersystem_users(update_ts);

CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
    id bigserial not null primary key,
    object_id varchar not null,
    object_value jsonb not null,
    update_ts timestamp not null,
    CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE(object_id)
);

CREATE INDEX IF NOT EXISTS oo_update_ts ON stg.ordersystem_orders(update_ts);

CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id bigserial not null primary key,
    object_id varchar not null,
    object_value jsonb not null,
    update_ts timestamp not null,
    CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE(object_id)
);

CREATE INDEX IF NOT EXISTS or_update_ts ON stg.ordersystem_restaurants(update_ts);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_deliveries(
    id bigserial not null primary key,
    object_id varchar not null,
    object_value jsonb not null,
    update_ts timestamp not null,
    CONSTRAINT deliverysystem_deliveries_object_id_uindex UNIQUE(object_id) 
);

CREATE INDEX IF NOT EXISTS deliverysystem_deliveries_update_ts
    ON stg.deliverysystem_deliveries(update_ts);

CREATE TABLE IF NOT EXISTS stg.deliverysystem_couriers(
    id bigserial not null primary key,
    object_id varchar not null,
    object_value jsonb not null,
    update_ts timestamp not null,
    CONSTRAINT deliverysystem_couriers_object_id_uindex UNIQUE(object_id)
);

CREATE INDEX IF NOT EXISTS deliverysystem_couriers_update_ts
    ON stg.deliverysystem_couriers(update_ts);
