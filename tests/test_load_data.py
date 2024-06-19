import pytest
from binance.error import ServerError
from requests_mock import Mocker

from application.main import load_data

TIME_FRAME = "1d"
PAIR = "BTCUSD"


def test_success(requests_mock: Mocker) -> None:
    response_data = ["1"]
    requests_mock.get('https://api.binance.com/api/v3/klines', json=response_data)

    result = load_data(TIME_FRAME, PAIR)

    assert response_data == result


@pytest.mark.parametrize("status_code", [500, 501, 502, 503, 504])
def test_server_error(status_code: int, requests_mock: Mocker) -> None:
    requests_mock.get('https://api.binance.com/api/v3/klines', status_code=status_code)

    with pytest.raises(ServerError):
        load_data(TIME_FRAME, PAIR)
