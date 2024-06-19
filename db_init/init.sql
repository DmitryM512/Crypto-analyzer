CREATE TABLE IF NOT EXISTS bid_ask_ratio (
    id SERIAL PRIMARY KEY,
    time VARCHAR(21),
    pair VARCHAR(12),
    bid_ask_ratio_total FLOAT,
    bid_ask_ratio_50 FLOAT,
    bid_ask_ratio_20 FLOAT,
    bid_ask_ratio_8 FLOAT,
    bid_ask_ratio_5 FLOAT,
    bid_ask_ratio_3 FLOAT,
    limit_density VARCHAR(512),
    total_orders FLOAT,
    sell_buy_ratio FLOAT,
    buy_sell_ratio FLOAT,
    delta FLOAT

);

CREATE TABLE IF NOT EXISTS signals (
    id SERIAL PRIMARY KEY,
    type VARCHAR(16),
    time_frame VARCHAR(4),
    instrument VARCHAR(12),
    time VARCHAR(21),
    volume FLOAT,
    extra VARCHAR(128),
    delta FLOAT,
    exchange VARCHAR(12),
    percent_change FLOAT

);

CREATE TABLE IF NOT EXISTS agg_trade
(
    id       SERIAL PRIMARY KEY,
    time     varchar(32),
    pair     varchar(16),
    buy_sum  float,
    sell_sum float
);
