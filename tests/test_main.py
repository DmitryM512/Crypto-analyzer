from datetime import datetime

from mock.mock import MagicMock
from psycopg2._psycopg import connection, cursor as cursor_type
from psycopg2.extras import DictCursor, DictRow
from pytest_mock import MockerFixture
from requests_mock import Mocker

from application.main import main, SignalType
from application.settings import settings


def test_success(connection_db: connection, requests_mock: Mocker, mocker: MockerFixture) -> None:
    line = [
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
    requests_mock.get('https://api.binance.com/api/v3/klines', json=[line, line, line])

    m_send_message: MagicMock = mocker.patch("telebot.TeleBot.send_message")

    main(connection_db, "1d")

    assert m_send_message.call_count == 1

    assert "chat_id" in m_send_message.call_args_list[0].kwargs
    assert m_send_message.call_args_list[0].kwargs["chat_id"] == settings.chat_ids[0]

    assert "text" in m_send_message.call_args_list[0].kwargs
    assert isinstance(m_send_message.call_args_list[0].kwargs["text"], str)

    with connection_db.cursor() as cursor:  # type: cursor_type
        cursor.execute("SELECT count(id) FROM signals")
        result: tuple[int] = cursor.fetchone()
        count: int = result[0]
    assert count == 1

    with connection_db.cursor(cursor_factory=DictCursor) as cursor:  # type: cursor_type
        cursor.execute("SELECT * FROM signals LIMIT 1")
        row: DictRow = cursor.fetchone()
    assert row['type'] == SignalType.CDL_PATTERN.value
    assert row['time_frame'] == "1d"
    assert row['pair'] == "BTCUSDT"
    assert row['time'] == 1699488000
    assert float(row['volume']) == 0.
    assert "id" in row
    assert "created" in row
    assert isinstance(row["created"], datetime)
    assert "extra" in row
    assert isinstance(row["extra"], dict)
