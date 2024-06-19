import json
import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

import pandas as pd
import numpy as np
from scipy import stats
import pandas_ta as ta  # noqa
import telebot
from binance.error import ServerError
from binance.spot import Spot
from psycopg2._psycopg import cursor as cursor_type

from application import constants
from application.exceptions import LowLinesCountException
from application.settings import settings

__all__ = ["main", 'Signal', 'SignalType', 'save_to_db', 'send_signal']

logger = logging.getLogger("crypto")


class SignalType(str, Enum):
    INCREASED_VOLUME = "INCREASED_VOLUME"
    VSA = 'VSA'
    FLAT_n_VOLUME = 'FLATnVOLUME'


@dataclass
class Signal:
    type: SignalType
    pair: str
    time_frame: str | int
    time: int
    volume: int
    percent_change: float
    created: datetime
    delta: float | None

    extra: dict | None = dict
    db_id: int | None = None

    @property
    def message(self) -> str:
        if self.type == SignalType.INCREASED_VOLUME:
            return (
                f"{self.pair}\n{self.time_frame}\n{self.time}\npattern: INCREASED VOLUME\n"
                f"% change: {self.percent_change}\nVO: {self.volume}%\nDelta: {self.delta}"
            )

        elif self.type == SignalType.VSA:
            return (
                f"{self.pair}\n{self.time_frame}\n{self.time}\npattern: VSA\n"
                f"vsa value: {self.extra.get('vsa_value')}\n% change: {self.percent_change}\n"
                f"VO: {self.volume}%\nDelta: {self.delta}"
            )

        elif self.type == SignalType.FLAT_n_VOLUME:
            return (
                f"{self.pair}\n{self.time_frame}\n{self.time}\npattern: FLATnVOLUME\n"
                f"mean value: {self.extra.get('flat_value')}\n% change: {self.percent_change}\n"
                f"VO: {self.volume}%\nDelta: {self.delta}"
            )

        else:
            raise ValueError(f"Unknown {self.type=}")


def main(db_connection, time_frame: str) -> None:
    for pair in settings.pairs:
        try:
            lines = load_data(time_frame, pair)
        except ServerError:
            logger.warning(f"Binance server error for {time_frame=} {pair=}")
            continue
        except Exception as e:
            logger.exception("Error occurred at loading data")
            continue

        try:
            signal = parse_data(time_frame, pair, lines)
        except LowLinesCountException:
            logger.warning(f"Got only one line for {time_frame=} {pair=}")
            continue
        except Exception as e:
            logger.exception(f"Error occurred at processing")
            continue

        try:
            if signal:
                signal.db_id = save_to_db(db_connection, signal)
        except Exception as e:
            logger.exception(f"Error occurred at saving {signal=} to DB")

        try:
            if signal:
                send_signal(signal)
        except Exception as e:
            logger.exception(f"Error occurred at sending {signal=}")


def load_data(time_frame: str, pair: str) -> list:
    binance_client = Spot(api_key=settings.binance_api_key, api_secret=settings.binance_api_secret)
    lines = binance_client.klines(pair, time_frame, limit=50)

    return lines


def parse_data(pair: str, time_frame: str, lines: list) -> Signal | None:
    if len(lines) < 2:
        raise LowLinesCountException()

    df = pd.DataFrame(lines, columns=constants.BINANCE_REQUIRE_COLUMNS)

    df["Date"] = pd.to_datetime(df["Date"], unit="ms")
    df["Date"] = df["Date"].astype(str)
    df = df.drop(columns=constants.BINANCE_DROP_COLUMNS)
    df[["Open price", "High price", "Low price", "Close price", "Volume", 'Taker buy base asset volume']] = df[
        ["Open price", "High price", "Low price", "Close price", "Volume", 'Taker buy base asset volume']
    ].astype(float)

    # VO
    df["Volume"] = df["Volume"].astype(int)
    df["EMA_5"] = df["Volume"].ewm(span=5, adjust=False).mean()
    df["EMA_10"] = df["Volume"].ewm(span=10, adjust=False).mean()
    df["VO"] = df['VO'] = (df['EMA_5'] - df['EMA_10']) / df['EMA_10'] * 100

    # SMI
    df['SMI'] = ta.smi(df['Close price'], fast=5, slow=20, signal=5)['SMIo_5_20_5']

    # Delta
    df['Taker sell base asset volume'] = df['Volume'] - df['Taker buy base asset volume']
    df['Delta'] = df['Taker buy base asset volume'] - df['Taker sell base asset volume']
    df['Normalized Delta'] = (df['Delta'] - df['Delta'].mean()) / df['Delta'].std()

    #VSA
    norm_lookback = 14
    atr = ta.atr(df['High price'], df['Low price'], df['Close price'], norm_lookback)
    vol_med = df['Volume'].rolling(norm_lookback).median()
    df['norm_range'] = (df['High price'] - df['Low price']) / atr
    df['norm_volume'] = df['Volume'] / vol_med
    norm_vol = df['norm_volume'].to_numpy()
    norm_range = df['norm_range'].to_numpy()
    range_dev = np.zeros(len(df))
    range_dev[:] = np.nan
    for i in range(norm_lookback * 2, len(df)):
        window = df.iloc[i - norm_lookback + 1: i + 1]
        slope, intercept, r_val, _, _ = stats.linregress(window['norm_volume'], window['norm_range'])
        if slope <= 0.0 or r_val < 0.2:
            range_dev[i] = 0.0
            continue

        pred_range = intercept + slope * norm_vol[i]
        range_dev[i] = norm_range[i] - pred_range


    # info to send
    time = df["Date"].iloc[-2]
    volume = df["VO"].iloc[-2].round(2)
    created = datetime.utcnow()
    smi = df['SMI'][-14:-2].loc[(df['SMI'] < 0.065 ) & (df['SMI'] > -0.065)].size
    vsa_value = range_dev[-2].round(2)
    delta = df['Normalized Delta'].iloc[-2].round(2) #Delta
    percent_change = ((df.iloc[-2]["Close price"] - df.iloc[-2]["Open price"]) \
                      / df.iloc[-2]["Close price"] * 100).round(2)

    signal = None
    signal_kwargs = dict(pair=pair, time_frame=time_frame, volume=volume, time=time, created=created, delta=delta,
                         percent_change=percent_change)

    if smi == 12 and volume > 10:
        flat_value = df["SMI"][-14:-2].loc[(df["SMI"] < 0.065) & (df["SMI"] > -0.065)].mean().round(3)
        signal = Signal(type=SignalType.FLAT_n_VOLUME, extra={"flat_value": flat_value}, **signal_kwargs)

    if (vsa_value >= 0.5 or vsa_value <= -0.5) and volume > 20:
        signal = Signal(type=SignalType.VSA, extra={"vsa_value": vsa_value}, **signal_kwargs)

    if volume > 20 and (delta > 2.8 or delta < -2.8):
        prev_percent_change = ((df.iloc[-3]["Close price"] - df.iloc[-3]["Open price"]) \
                          / df.iloc[-3]["Close price"] * 100).round(2)
        signal = Signal(type=SignalType.INCREASED_VOLUME, extra={'prev_percent_change': prev_percent_change},
                        **signal_kwargs)

    logger.info(f"{pair} {time_frame} pattern was {'' if signal else 'not'} found")

    return signal


def save_to_db(db_connection, signal: Signal, exchange='Binance') -> int:

    query = """
    INSERT INTO signals (type, time_frame, instrument, time, volume, extra, delta, exchange, percent_change)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    RETURNING id;
    """

    values = (signal.type.value, signal.pair, signal.time_frame,
              signal.time, signal.volume, json.dumps(signal.extra), signal.delta, exchange, signal.percent_change)

    with db_connection.cursor() as cursor:  # type: cursor_type
        cursor.execute(query, values)
        db_connection.commit()
        row = cursor.fetchone()

        return row[0]


def send_signal(signal: Signal) -> None:
    bot = telebot.TeleBot(settings.bot_token)

    for chat_id in settings.chat_ids:
        try:
            bot.send_message(chat_id=chat_id, text=signal.message)
        except Exception:
            logger.exception(f"Error occurred at sending {signal.message=} to {chat_id=}")
