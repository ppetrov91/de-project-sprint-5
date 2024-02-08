CREATE SCHEMA IF NOT EXISTS dds;

CREATE TABLE IF NOT EXISTS dds.srv_wf_settings(
    id bigserial not null primary key,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSONB NOT NULL
);

create table if not exists dds.dm_users (
    id bigserial primary key,
    user_id varchar not null,
    user_name varchar not null,
    user_login varchar not null,
    CONSTRAINT dm_users_user_id_ukey UNIQUE(user_id)
);

create table if not exists dds.dm_restaurants (
    id bigserial not null primary key,
    restaurant_id varchar not null,
    restaurant_name varchar not null,
    active_from timestamp not null,
    active_to timestamp not null,
    CONSTRAINT dm_restaurants_rid_af_ukey UNIQUE (restaurant_id, active_from),
    CONSTRAINT dm_restaurants_rid_at_ukey UNIQUE (restaurant_id, active_to)
);

create table if not exists dds.dm_couriers (
    id bigserial not null primary key,
    courier_id VARCHAR(24) not null UNIQUE,
    courier_name VARCHAR(255) not null,
    courier_surname VARCHAR(255) not null
);

create table if not exists dds.dm_timestamps (
    id bigserial not null primary key,
    ts timestamp not null,
    year smallint not null,
    month smallint not null,
    day smallint not null,
    "time" time not null,
    "date" date not null,
    CONSTRAINT dm_timestamps_year_check CHECK (year >= 2022 and year < 2500),
    CONSTRAINT dm_timestamps_month_check CHECK (month BETWEEN 1 AND 12),
    CONSTRAINT dm_timestamps_day_check CHECK (day BETWEEN 1 AND 31),
    CONSTRAINT dm_timestamps_ts_ukey UNIQUE (ts)
);

create table if not exists dds.dm_products (
    id bigserial not null primary key,
    restaurant_id bigint not null,
    product_id varchar not null,
    product_name varchar not null,
    product_price numeric(14,2) not null default 0,
    active_from timestamp not null,
    active_to timestamp not null,
    constraint dm_products_product_price_check CHECK(product_price >= 0),
    constraint dm_prod_prod_id_af_ukey UNIQUE(product_id, active_from),
    constraint dm_prod_prod_id_at_ukey UNIQUE(product_id, active_to),
    constraint dm_prod_restaurant_id_fkey FOREIGN KEY(restaurant_id) REFERENCES dds.dm_restaurants(id)
);

CREATE INDEX IF NOT EXISTS dm_prod_rest_id_ix
    ON dds.dm_products(restaurant_id);

create table if not exists dds.dm_orders(
    id bigserial not null primary key,
    user_id bigint not null,
    restaurant_id bigint not null,
    timestamp_id bigint not null,
    order_key varchar not null,
    order_status varchar not null,
    CONSTRAINT dm_orders_order_key_uindex UNIQUE(order_key),
    CONSTRAINT dm_orders_user_id_fkey FOREIGN KEY (user_id) REFERENCES dds.dm_users(id),
    CONSTRAINT dm_orders_restaurant_id_fkey FOREIGN KEY (restaurant_id) REFERENCES dds.dm_restaurants(id),
    CONSTRAINT dm_orders_timestamp_id_fkey FOREIGN KEY (timestamp_id) REFERENCES dds.dm_timestamps(id)
);

CREATE INDEX IF NOT EXISTS dm_orders_timestamp_id_ix
    ON dds.dm_orders(timestamp_id);

CREATE INDEX IF NOT EXISTS dm_orders_user_id_ix
    ON dds.dm_orders(user_id);

CREATE INDEX IF NOT EXISTS dm_orders_restaurant_id_ix
    ON dds.dm_orders(restaurant_id);

create table if not exists dds.fct_product_sales(
    id bigserial not null primary key,
    product_id bigint not null,
    order_id bigint not null,
    product_count integer not null default 0,
    product_price numeric(14,2) not null default 0,
    total_sum numeric(14,2) not null default 0,
    bonus_payment numeric(14,2) not null default 0,
    bonus_grant numeric(14,2) not null default 0,
    CONSTRAINT fct_product_sales_product_id_order_id_ukey UNIQUE(product_id, order_id),
    CONSTRAINT fct_product_sales_count_check CHECK (count >= 0),
    CONSTRAINT fct_product_sales_price_check CHECK (price >= 0),
    CONSTRAINT fct_product_sales_total_sum_check CHECK (total_sum >= 0),
    CONSTRAINT fct_product_sales_bonus_payment_check CHECK (bonus_payment >= 0),
    CONSTRAINT fct_product_sales_bonus_grant_check CHECK (bonus_grant >= 0),
    CONSTRAINT fct_product_sales_product_id_fkey FOREIGN KEY(product_id) REFERENCES dds.dm_products(id),
    CONSTRAINT fct_product_sales_order_id_fkey FOREIGN KEY(order_id) REFERENCES dds.dm_orders(id)
);

CREATE INDEX IF NOT EXISTS ddf_fps_order_id_ix
    ON dds.fct_product_sales(order_id);

CREATE INDEX IF NOT EXISTS ddf_fps_product_id_ix
    ON dds.fct_product_sales(product_id);  
