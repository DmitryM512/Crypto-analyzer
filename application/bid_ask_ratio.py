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
import numpy as np
from binance.spot import Spot
import constants
from datetime import timezone
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

# executors = {
#     'default': ThreadPoolExecutor(20),
#     'processpool': ProcessPoolExecutor(2)
# }
# job_defaults = {
#     'coalesce': False,
#     'max_instances': 3
# }

scheduler = BlockingScheduler(timezone=utc)

logger = logging.getLogger("main")

@dataclass
class Signal:
    time: int
    pair: str
    bid_ask_ratio_total: float
    bid_ask_ratio_50: float
    bid_ask_ratio_20: float
    bid_ask_ratio_8: float
    bid_ask_ratio_5: float
    bid_ask_ratio_3: float
    sell_buy_ratio: float
    depth_50: int
    depth_20: int
    depth_8: int
    depth_5: int
    depth_3: int
    price: float
    limit_density: dict | None
    total_orders: float
    buy_sell_ratio: float
    delta: float
    # bid_density_total: float
    # ask_density_total: float
    # bid_density_50: float
    # ask_density_50: float
    # bid_density_20: float
    # ask_density_20: float
    # bid_density_8: float
    # ask_density_8: float
    # bid_density_5: float
    # ask_density_5: float
    # bid_density_3: float
    # ask_density_3: float


    db_id: int | None = None

    # @property
    # def message(self) -> str:
    #     if self.pair in ["BTCUSDT"]:
    #         return f'{self.pair}\n{self.time}\nbid ask ratio total: {self.bid_ask_ratio_total}' \
    #                f'\nbid ask ratio 50%: {self.bid_ask_ratio_50}\nbid ask ratio 20%: {self.bid_ask_ratio_20}\n' \
    #                f'{self.limit_density}\nb/s ratio: {self.buy_sell_ratio} s/b ratio: {self.sell_buy_ratio}\n' \
    #                f'delta: {self.delta}'
    #     if self.pair in ['ETHUSDT'] and (self.bid_ask_ratio_total < -0.4 or self.bid_ask_ratio_total > 0.4):
    #         return f'{self.pair}\n{self.time}\nbid ask ratio total: {self.bid_ask_ratio_total}' \
    #                f'\nbid ask ratio 50%: {self.bid_ask_ratio_50}\nbid ask ratio 20%: {self.bid_ask_ratio_20}\n' \
    #                f'{self.limit_density}\nb/s ratio: {self.buy_sell_ratio} s/b ratio: {self.sell_buy_ratio}\n' \
    #                f'delta: {self.delta}'
    #     if self.pair in ["SOLUSDT"] and (self.bid_ask_ratio_8 > 0.4 or self.bid_ask_ratio_8 < -0.4
    #                                      or self.bid_ask_ratio_5 > 0.4 or self.bid_ask_ratio_5 < -0.4):
    #         return f'{self.pair}\n{self.time}\nbid ask ratio 20%: {self.bid_ask_ratio_20}' \
    #                f'\nbid ask ratio 8%: {self.bid_ask_ratio_8}\nbid ask ratio 5%: {self.bid_ask_ratio_5}\n' \
    #                f'{self.limit_density}\nb/s ratio: {self.buy_sell_ratio} s/b ratio: {self.sell_buy_ratio}\n' \
    #                f'delta: {self.delta}'


def depth_main(db_connection) -> None:
    for pair in settings.pairs_depth:
        try:
            data = load_depth_data(pair)
        except ServerError:
            logger.warning(f"Binance server error for {pair=}")
            continue
        except Exception as e:
            logger.exception("Error occurred at loading data")
            continue
        try:
            market_data = load_market_data(pair)
        except ServerError:
            logger.warning(f"Binance server error for {pair=}")
            continue
        except Exception as e:
            logger.exception("Error occurred at loading data")
            continue

        try:
            signal = parse_depth_data(data, market_data, pair)
        except Exception as e:
            logger.exception(f"Error occurred at processing  {pair=}")
            continue

        try:
            if signal:
                signal.db_id = save_to_db(db_connection, signal)
        except Exception as e:
            logger.exception(f"Error occurred at saving {signal=} to DB")

        # try:
        #     if signal:
        #         send_signal_depth(signal)
        # except Exception as e:
        #     logger.exception(f"Error occurred at sending {signal=}")


def load_depth_data(pair: str) -> dict:
    client = Client(api_key=settings.binance_api_key, api_secret=settings.binance_api_secret)
    data = client.get_order_book(symbol=pair, limit=5000)
    return data


def load_market_data(pair: str) -> list:
    client = Spot(api_key=settings.binance_api_key, api_secret=settings.binance_api_secret)
    market_data = client.klines(symbol=pair, interval='1m', limit=50)
    return market_data


def parse_depth_data(data: dict, market_data: list, pair: str) -> Signal | None:
    bids = data['bids']
    asks = data['asks']
    df_bids = pd.DataFrame(bids)
    df_asks = pd.DataFrame(asks)
    df = pd.concat([df_bids, df_asks], axis=1)
    df.columns = ['price_bid', 'amount_bid', 'price_ask', 'amount_ask']
    df = df.dropna()
    df[['price_bid', 'amount_bid', 'price_ask', 'amount_ask']] = df[
        ['price_bid', 'amount_bid', 'price_ask', 'amount_ask']].astype(float)
    price = df['price_bid'].iloc[0]
    depth_50 = round(len(df) * 0.5)
    depth_20 = round(len(df) * 0.2)
    depth_8 = round(len(df) * 0.08)
    depth_5 = round(len(df) * 0.05)
    depth_3 = round(len(df) * 0.03)
    time = datetime.datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
    bid_ask_ratio_total = ((df['amount_bid'].sum() - df['amount_ask'].sum()) / (
            df['amount_bid'].sum() + df['amount_ask'].sum())).round(2)
    bid_ask_ratio_50 = ((df['amount_bid'][:depth_50].sum() - df['amount_ask'][:depth_50].sum()) / (
            df['amount_bid'][:depth_50].sum() + df['amount_ask'][:depth_50].sum())).round(2)
    bid_ask_ratio_20 = ((df['amount_bid'][:depth_20].sum() - df['amount_ask'][:depth_20].sum()) / (
            df['amount_bid'][:depth_20].sum() + df['amount_ask'][:depth_20].sum())).round(2)
    bid_ask_ratio_8 = ((df['amount_bid'][:depth_8].sum() - df['amount_ask'][:depth_8].sum()) / (
            df['amount_bid'][:depth_8].sum() + df['amount_ask'][:depth_8].sum())).round(2)
    bid_ask_ratio_5 = ((df['amount_bid'][:depth_5].sum() - df['amount_ask'][:depth_5].sum()) / (
            df['amount_bid'][:depth_5].sum() + df['amount_ask'][:depth_5].sum())).round(2)
    bid_ask_ratio_3 = ((df['amount_bid'][:depth_3].sum() - df['amount_ask'][:depth_3].sum()) / (
            df['amount_bid'][:depth_3].sum() + df['amount_ask'][:depth_3].sum())).round(2)
    total_orders = (df['amount_bid'].sum() + df['amount_ask'].sum()).round(2)
    # #mean density
    # bid_density_total = df['amount_bid'].mean().round(2)
    # ask_density_total = df['amount_ask'].mean().round(2)
    # bid_density_50 = df['amount_bid'][:depth_50].mean().round(2)
    # ask_density_50 = df['amount_ask'][:depth_50].mean().round(2)
    # bid_density_20 = df['amount_bid'][:depth_20].mean().round(2)
    # ask_density_20 = df['amount_ask'][:depth_20].mean().round(2)
    # bid_density_8 = df['amount_bid'][:depth_8].mean().round(2)
    # ask_density_8 = df['amount_ask'][:depth_8].mean().round(2)
    # bid_density_5 = df['amount_bid'][:depth_5].mean().round(2)
    # ask_density_5 = df['amount_ask'][:depth_5].mean().round(2)
    # bid_density_3 = df['amount_bid'][:depth_3].mean().round(2)
    # ask_density_3 = df['amount_ask'][:depth_3].mean().round(2)

    market_df = pd.DataFrame(market_data, columns=constants.BINANCE_REQUIRE_COLUMNS)
    market_df[["Open price", "High price", "Low price", "Close price", "Volume", 'Taker buy base asset volume']] = \
    market_df[["Open price", "High price", "Low price", "Close price", "Volume", 'Taker buy base asset volume']
    ].astype(float)
    market_df['Taker sell base asset volume'] = market_df['Volume'] - market_df['Taker buy base asset volume']
    market_df['Sell_buy_ratio'] = market_df['Taker sell base asset volume'] / market_df['Taker buy base asset volume']
    market_df['Buy_sell_ratio'] = market_df['Taker buy base asset volume'] / market_df['Taker sell base asset volume']
    market_df['Delta'] = market_df['Taker buy base asset volume'] - market_df['Taker sell base asset volume']
    sell_buy_ratio = market_df['Sell_buy_ratio'].iloc[-2].round(2)
    buy_sell_ratio = market_df['Buy_sell_ratio'].iloc[-2].round(2)
    delta = market_df['Delta'].iloc[-2].round(2)

    limit_density = None
    # if pair == 'BTCUSDT':
    #     df['aggregated_bid'] = (np.floor(df['price_bid'] / 20.0) * 20).astype(int)  # norm aggregation step
    #     df['aggregated_ask'] = (np.ceil(df['price_ask'] / 20.0) * 20).astype(int)
    #     bid_limit_df = df.groupby('aggregated_bid', as_index=False)['amount_bid'].sum().round(2)
    #     ask_limit_df = df.groupby('aggregated_ask', as_index=False)['amount_ask'].sum().round(2)
    #     result_bid = bid_limit_df[bid_limit_df['amount_bid'] > 50.0].set_index('aggregated_bid').to_dict()
    #     result_ask = ask_limit_df[ask_limit_df['amount_ask'] > 50.0].set_index('aggregated_ask').to_dict()
    #     str_ask = result_ask['amount_ask']
    #     str_bid = result_bid['amount_bid']
    #     limit_density = f'ask: {str_ask}\nbid: {str_bid}'

    signal_kwargs = dict(time=time, pair=pair, bid_ask_ratio_total=bid_ask_ratio_total,
                         bid_ask_ratio_50=bid_ask_ratio_50,
                         bid_ask_ratio_20=bid_ask_ratio_20, bid_ask_ratio_8=bid_ask_ratio_8,
                         bid_ask_ratio_5=bid_ask_ratio_5, bid_ask_ratio_3=bid_ask_ratio_3, price=price,
                         depth_50=depth_50, depth_20=depth_20, depth_8=depth_8, depth_5=depth_5, depth_3=depth_3,
                         limit_density=limit_density, total_orders=total_orders, sell_buy_ratio=sell_buy_ratio,
                         buy_sell_ratio=buy_sell_ratio, delta=delta)
                         # bid_density_total=bid_density_total,
                         # ask_density_total=ask_density_total, bid_density_50=bid_density_50, ask_density_50=ask_density_50,
                         # bid_density_20=bid_density_20, ask_density_20=ask_density_20, bid_density_8=bid_density_8,
                         # ask_density_8=ask_density_8, bid_density_5=bid_density_5, ask_density_5=ask_density_5,
                         # bid_density_3=bid_density_3, ask_density_3=ask_density_3)
    signal = Signal(**signal_kwargs)

    logger.info(f"{pair} {time}  was processed")
    return signal


def save_to_db(db_connection, signal: Signal) -> int:
    query = """
    INSERT INTO bid_ask_ratio (time, pair, bid_ask_ratio_total, bid_ask_ratio_50, bid_ask_ratio_20, 
     bid_ask_ratio_8,  bid_ask_ratio_5,  bid_ask_ratio_3, limit_density, total_orders, sell_buy_ratio,
      buy_sell_ratio, delta)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """

    values = (signal.time, signal.pair, signal.bid_ask_ratio_total, signal.bid_ask_ratio_50,
              signal.bid_ask_ratio_20, signal.bid_ask_ratio_8,
              signal.bid_ask_ratio_5, signal.bid_ask_ratio_3, signal.limit_density,
              signal.total_orders, signal.sell_buy_ratio, signal.buy_sell_ratio, signal.delta)

    with db_connection.cursor() as cursor:  # type: cursor_type
        cursor.execute(query, values)
        db_connection.commit()
        row = cursor.fetchone()

        return row[0]


# def send_signal_depth(signal: Signal) -> None:
#     bot = telebot.TeleBot(settings.bot_token_depth)
#     if signal.message:
#         for chat_id in settings.chat_ids_depth:
#             try:
#                 bot.send_message(chat_id=chat_id, text=signal.message)
#             except Exception:
#                 logger.exception(f"Error occurred at sending {signal.message=} to {chat_id=}")


with connect_db() as db_connection:
    job_h = scheduler.add_job(depth_main, 'cron', hour='*', minute='*', second='1', args=[db_connection], max_instances=3)
    scheduler.start()
