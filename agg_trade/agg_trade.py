import datetime
import logging
from apscheduler.schedulers.background import BlockingScheduler
import pandas as pd
from pytz import utc
from binance.client import Client
from settings import settings
from dataclasses import dataclass
from psycopg2._psycopg import cursor as cursor_type
from binance.error import ServerError
from db_back import connect_db
from datetime import timezone

scheduler = BlockingScheduler(timezone=utc)

logger = logging.getLogger("agg_trade")


@dataclass
class Agg:
    time: int
    pair: str
    buy_sum: float
    sell_sum: float
    buy_recent_qty: float
    sell_recent_qty: float
    buy_recent_count: int
    sell_recent_count: int

    db_id: int | None = None


def agg_main(db_connection) -> None:
    for pair in settings.pairs_depth:
        try:
            data = load_agg_trade(pair)
        except ServerError:
            logger.warning(f"Binance server error for {pair=}")
            continue
        except Exception as e:
            logger.exception("Error occurred at loading data")
            continue
        try:
            recent_trades = load_recent_trades(pair)
        except ServerError:
            logger.warning(f"Binance server error for {pair=}")
            continue
        except Exception as e:
            logger.exception("Error occurred at loading recent trades")
            continue
        try:
            agg_data = parse_agg_trade(data, recent_trades, pair)
        except Exception as e:
            logger.exception(f"Error occurred at processing  {pair=}")
            continue
        try:
            if agg_data:
                agg_data.db_id = save_to_db(db_connection, agg_data)
        except Exception as e:
            logger.exception(f"Error occurred at saving {agg_data=} to DB")


def load_agg_trade(pair: str) -> dict:
    client = Client(api_key=settings.binance_api_key, api_secret=settings.binance_api_secret)
    trades = client.get_aggregate_trades(symbol=pair)
    return trades


def load_recent_trades(pair: str) -> dict:
    client = Client(api_key=settings.binance_api_key, api_secret=settings.binance_api_secret)
    recent_trades = client.get_historical_trades(symbol=pair, limit=1000)
    return recent_trades


def parse_agg_trade(trades: dict, recent_trades: dict, pair: str) -> Agg | None:
    time = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    df_trades = pd.DataFrame(trades)
    df_trades['T'] = pd.to_datetime(df_trades['T'], unit='ms')
    df_trades['q'] = df_trades['q'].astype(float)
    agg_df = df_trades.groupby('m')['q'].sum().round(2)
    sell = agg_df.loc[True]
    buy = agg_df.loc[False]
    df_recent = pd.DataFrame(recent_trades)
    df_recent['time'] = pd.to_datetime(df_recent['time'], unit='ms')
    df_recent[['price', 'qty']] = df_recent[['price', 'qty']].astype(float)
    df_recent['time'] = df_recent['time'].dt.floor('15s')
    tmp_recent = df_recent.groupby(['time', 'isBuyerMaker']).agg({'id': 'count', 'qty': 'sum'})
    all_categories = tmp_recent.index.get_level_values(0)
    all_subcategories = [True, False]
    full_index = pd.MultiIndex.from_product([all_categories, all_subcategories], names=['time', 'isBuyerMaker'])
    rsl_recent = tmp_recent.reindex(full_index, fill_value=0)[:2]
    buy_recent_qty = rsl_recent.iloc[1, 1].round(3).astype(float)
    sell_recent_qty = rsl_recent.iloc[0, 1].round(3).astype(float)
    buy_recent_count = int(rsl_recent.iloc[1, 0])
    sell_recent_count = int(rsl_recent.iloc[0, 0])
    signal_kwargs = dict(time=time, pair=pair, buy_sum=buy, sell_sum=sell, buy_recent_qty=buy_recent_qty,
                         sell_recent_qty=sell_recent_qty, buy_recent_count=buy_recent_count,
                         sell_recent_count=sell_recent_count)
    data = Agg(**signal_kwargs)

    return data


def save_to_db(db_connection, data: Agg) -> int:
    query = """
    INSERT INTO agg_trade (time, pair, buy_sum, sell_sum, buy_recent_qty, sell_recent_qty, buy_recent_count, 
    sell_recent_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """

    values = (data.time, data.pair, data.buy_sum, data.sell_sum, data.buy_recent_qty, data.sell_recent_qty,
              data.buy_recent_count, data.sell_recent_count)

    with db_connection.cursor() as cursor:  # type: cursor_type
        cursor.execute(query, values)
        db_connection.commit()
        row = cursor.fetchone()

        return row[0]


with connect_db() as db_connection:
    job_h = scheduler.add_job(agg_main, 'cron', hour='*', minute='*', second='*/15', args=[db_connection], max_instances=3)
    scheduler.start()




