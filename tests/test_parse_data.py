from datetime import datetime

import pytest
from mock.mock import MagicMock
from pytest_mock import MockerFixture

from application.exceptions import LowLinesCountException
from application.main import parse_data, SignalType

PAIR = "BTCUSD"
TIME_FRAME = "1d"
LINE = [
    1699488000000,
    "35624.72000000",
    "37972.24000000",
    "35534.05000000",
    "36701.09000000",
    "82537.88885000",
    1699574399999,
    "3033417922.78268170",
    2138765,
    "42156.81236000",
    "1550254981.04518770",
    "0"
]


def test_success_volume() -> None:
    signal = parse_data(PAIR, TIME_FRAME, [LINE, LINE])

    assert signal.type == SignalType.CDL_PATTERN
    assert signal.pair == PAIR
    assert signal.time_frame == TIME_FRAME
    assert signal.created is not None
    assert isinstance(signal.created, datetime)


def test_success_increased_volume(mocker: MockerFixture) -> None:
    m_calculate_ema: MagicMock = mocker.patch("application.main._calculate_ema")
    m_calculate_ema.return_value = 100

    signal = parse_data(PAIR, TIME_FRAME, [LINE, LINE])

    assert signal.type == SignalType.INCREASED_VOLUME
    assert signal.pair == PAIR
    assert signal.time_frame == TIME_FRAME
    assert signal.created is not None
    assert isinstance(signal.created, datetime)


def test_success_ppo(mocker: MockerFixture) -> None:
    m_calculate_ppo: MagicMock = mocker.patch("application.main._calculate_ppo")
    m_calculate_ppo.return_value = 12

    signal = parse_data(PAIR, TIME_FRAME, [LINE, LINE])

    assert signal.type == SignalType.PPO
    assert signal.pair == PAIR
    assert signal.time_frame == TIME_FRAME
    assert signal.created is not None
    assert isinstance(signal.created, datetime)


def test_failed_one_line() -> None:
    with pytest.raises(LowLinesCountException):
        parse_data(PAIR, TIME_FRAME, [LINE])
