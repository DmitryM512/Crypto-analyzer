import logging
from datetime import datetime
import pandas as pd
import numpy as np
from scipy import stats
import pandas_ta as ta  # noqa
from binance.error import ServerError
import requests as re
import datetime as dt
import apimoex
import telebot
from application.exceptions import LowLinesCountException
from application.settings import settings

from application.main import Signal, SignalType, save_to_db, send_signal

__all__ = ['main_moex']

logger = logging.getLogger("moex")


def main_moex(db_connection, interval: int) -> None:
    for security in settings.moex_pairs:
        try:
            data = load_data_moex(security, interval)
        except ServerError:
            logger.warning(f"Binance server error for {interval=} {security=}")
            continue
        except Exception as e:
            logger.exception("Error occurred at loading data")
            continue

        try:
            signal = parse_data_moex(security, interval, data)
        except LowLinesCountException:
            logger.warning(f"Got only one line for {interval=} {security=}")
            continue
        except Exception as e:
            logger.exception(f"Error occurred at processing {security}")
            continue

        try:
            if signal:
                signal.db_id = save_to_db(db_connection, signal, exchange='MOEX')
        except Exception as e:
            logger.exception(f"Error occurred at saving {signal=} to DB")

        try:
            if signal:
                send_signal_moex(signal)
        except Exception as e:
            logger.exception(f"Error occurred at sending {signal=}")

def send_signal_moex(signal: Signal) -> None:
    bot = telebot.TeleBot(settings.bot_token_moex)

    for chat_id in settings.chat_ids_moex:
        try:
            bot.send_message(chat_id=chat_id, text=signal.message)
        except Exception:
            logger.exception(f"Error occurred at sending {signal.message=} to {chat_id=}")


def load_data_moex(security: str, interval: int) -> list:
    start = dt.datetime.today() - dt.timedelta(days=50)

    with re.Session() as session:
        data = apimoex.get_board_candles(session=session, security=security, interval=interval, start=str(start))
    return data


def parse_data_moex(security: str, interval: int, data: list) -> Signal | None:

    df = pd.DataFrame(data)
    df["begin"] = pd.to_datetime(df["begin"])
    df[['open', 'close', 'high', 'low', 'value']] = df[['open', 'close', 'high', 'low', 'value']].astype(float)

    # VO
    # df["Volume"] = df["Volume"].astype(int)
    df["EMA_5"] = df["value"].ewm(span=5, adjust=False).mean()
    df["EMA_10"] = df["value"].ewm(span=10, adjust=False).mean()
    df["VO"] = (df['EMA_5'] - df['EMA_10']) / df['EMA_10'] * 100

    # SMI
    df['SMI'] = ta.smi(df['close'], fast=5, slow=20, signal=5)['SMIo_5_20_5']

    # VSA
    norm_lookback = 14
    atr = ta.atr(df['high'], df['low'], df['close'], norm_lookback)
    vol_med = df['value'].rolling(norm_lookback).median()
    df['norm_range'] = (df['high'] - df['low']) / atr
    df['norm_volume'] = df['value'] / vol_med
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
    time = df["begin"].iloc[-1]
    volume = df["VO"].iloc[-1].round(2)
    created = datetime.utcnow()
    smi = df['SMI'][-13:-1].loc[(df['SMI'] < 0.065) & (df['SMI'] > -0.065)].size
    vsa_value = range_dev[-1].round(2)
    percent_change = ((df.iloc[-1]["close"] - df.iloc[-1]["open"]) \
                      / df.iloc[-1]["close"] * 100).round(2)
    delta = None
    signal = None
    signal_kwargs = dict(pair=interval, time_frame=security, volume=volume, time=time, created=created, delta = delta,
                         percent_change=percent_change)

    if interval == 60:
        if smi == 12 and volume > 30:
            flat_value = df["SMI"][-13:-1].loc[(df["SMI"] < 0.065) & (df["SMI"] > -0.065)].mean().round(3)
            signal = Signal(type=SignalType.FLAT_n_VOLUME, extra={"flat_value": flat_value}, **signal_kwargs)

        if (vsa_value >= 0.5 or vsa_value <= -0.5) and volume > 30:
            signal = Signal(type=SignalType.VSA, extra={"vsa_value": vsa_value}, **signal_kwargs)

        if volume > 50:
            prev_percent_change = ((df.iloc[-2]["close"] - df.iloc[-2]["open"]) \
                                   / df.iloc[-2]["close"] * 100).round(3)
            signal = Signal(type=SignalType.INCREASED_VOLUME, **signal_kwargs,
                            extra={"prev_percent_change": prev_percent_change})
    else:
        if smi == 12 and volume > 10:
            flat_value = df["SMI"][-13:-1].loc[(df["SMI"] < 0.065) & (df["SMI"] > -0.065)].mean().round(3)
            signal = Signal(type=SignalType.FLAT_n_VOLUME, extra={"flat_value": flat_value}, **signal_kwargs)

        if (vsa_value >= 0.5 or vsa_value <= -0.5) and volume > 20:
            signal = Signal(type=SignalType.VSA, extra={"vsa_value": vsa_value}, **signal_kwargs)

        if volume > 20:
            prev_percent_change = ((df.iloc[-2]["close"] - df.iloc[-2]["open"]) \
                              / df.iloc[-2]["close"] * 100).round(3)
            signal = Signal(type=SignalType.INCREASED_VOLUME, **signal_kwargs,
                            extra={"prev_percent_change": prev_percent_change})

    logger.info(f"{security} {interval} pattern was {'' if signal else 'not'} found")

    return signal


