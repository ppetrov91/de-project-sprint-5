CREATE SCHEMA IF NOT EXISTS cdm;

CREATE TABLE IF NOT EXISTS cdm.srv_wf_settings(
    id bigserial not null primary key,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report (
    id bigserial primary key,
    restaurant_id character varying(24) NOT NULL,
    restaurant_name character varying(50) NOT NULL,
    settlement_date date NOT NULL,
    orders_count integer DEFAULT 0 NOT NULL,
    orders_total_sum numeric(14,2) DEFAULT 0 NOT NULL,
    orders_bonus_payment_sum numeric(14,2) DEFAULT 0 NOT NULL,
    orders_bonus_granted_sum numeric(14,2) DEFAULT 0 NOT NULL,
    order_processing_fee numeric(14,2) DEFAULT 0 NOT NULL,
    restaurant_reward_sum numeric(14,2) DEFAULT 0 NOT NULL,
    CONSTRAINT restaurant_id_settlement_date_uk UNIQUE(restaurant_id, settlement_date),
    CONSTRAINT dm_settlement_report_order_processing_fee_check CHECK (order_processing_fee >= 0),
    CONSTRAINT dm_settlement_report_orders_bonus_granted_sum_check CHECK (orders_bonus_granted_sum >= 0),
    CONSTRAINT dm_settlement_report_orders_bonus_payment_sum_check CHECK (orders_bonus_payment_sum >= 0),
    CONSTRAINT dm_settlement_report_orders_count_check CHECK (orders_count >= 0),
    CONSTRAINT dm_settlement_report_orders_total_sum_check CHECK (orders_total_sum >= 0),
    CONSTRAINT dm_settlement_report_restaurant_reward_sum_check CHECK (restaurant_reward_sum >= 0),
    CONSTRAINT dm_settlement_report_settlement_date_check CHECK (settlement_date >= '2022-01-01' AND settlement_date <= '2499-12-31')
);

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
   id bigserial primary key,
   courier_id bigint not null,
   courier_name varchar not null,
   settlement_year smallint not null,
   settlement_month smallint not null,
   orders_count int DEFAULT 0 NOT NULL,
   orders_total_sum numeric(14,2) DEFAULT 0 NOT NULL,
   rate_avg numeric(4,2) DEFAULT 0 NOT NULL,
   order_processing_fee numeric(14,2) DEFAULT 0 NOT NULL,
   courier_order_sum numeric(14,2) DEFAULT 0 NOT NULL,
   courier_tips_sum numeric(14,2) DEFAULT 0 NOT NULL,
   courier_reward_sum numeric(14,2) DEFAULT 0 NOT NULL,
   CONSTRAINT dm_courier_ledger_courier_id_settlement_year_month_ukey UNIQUE(courier_id, settlement_year, settlement_month),
   CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (settlement_year BETWEEN 2022 AND 2499),
   CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (settlement_month BETWEEN 1 AND 12),
   CONSTRAINT dm_courier_ledger_orders_count_check CHECK (orders_count >= 0),
   CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK (orders_total_sum >= 0),
   CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (rate_avg >= 0),
   CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK (order_processing_fee >= 0),
   CONSTRAINT dm_courier_ledger_order_courier_order_sum_check CHECK (courier_order_sum >= 0),
   CONSTRAINT dm_courier_ledger_order_courier_tips_sum_check CHECK (courier_tips_sum >= 0),
   CONSTRAINT dm_courier_ledger_order_courier_reward_sum_check CHECK (courier_reward_sum >= 0)
);
