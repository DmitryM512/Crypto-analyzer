from enum import Enum
import os

from pydantic_settings import BaseSettings, SettingsConfigDict

__all__ = ["settings", "LogFormatEnum"]


class LogFormatEnum(str, Enum):
    json = "json"
    text = "text"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", env_prefix="app_", case_sensitive=False
    )

    bot_token: str = os.getenv('BOT_TOKEN')
    bot_token_moex: str = os.getenv('BOT_TOKEN_MOEX')
    bot_token_depth: str = os.getenv('BOT_TOKEN_DEPTH')
    binance_api_key: str = os.getenv('API_KEY')
    binance_api_secret: str = os.getenv('API_KEY_SECRET')


    pairs: list[str] = [
        "BTCUSDT",
        "ETHUSDT",
        "BNBUSDT",
        "SOLUSDT",
        "ADAUSDT",
        "AVAXUSDT",
        "DOGEUSDT",
        "DOTUSDT",
        "TRXUSDT",
        "LINKUSDT",
        "MATICUSDT",
        "ICPUSDT",
        "LTCUSDT",
        "BCHUSDT",
        "ATOMUSDT",
        "UNIUSDT",
        "NEARUSDT",
        "APTUSDT",
        "INJUSDT",
        "OPUSDT",
        "FILUSDT",
        "ETCUSDT"

    ]
    chat_ids: list[int] = os.getenv('CHAT_ID')
    chat_ids_moex: list[int] = os.getenv('CHAT_ID_MOEX')
    chat_ids_depth: list[int] = os.getenv('CHAT_ID_DEPTH')

    log_level: str = "INFO"
    log_format: LogFormatEnum = LogFormatEnum.json

    db_dsn: str = 'dbname=main user=analyzer password=analyzer host=pg port=5432'

    moex_pairs: list[str] = ['ABIO', 'ABRD', 'AFKS', 'AFLT', 'AGRO', 'AKRN', 'ALRS', 'AMEZ', 'APTK', 'AQUA', 'ARSA',
                             'ASSB', 'ASTR', 'AVAN', 'BANE', 'BANEP', 'BELU', 'BISVP', 'BLNG', 'BRZL', 'BSPB', 'BSPBP',
                             'CARM', 'CBOM', 'CHGZ', 'CHKZ', 'CHMF', 'CHMK', 'CIAN', 'CNTL', 'CNTLP', 'DIOD', 'DSKY',
                             'DVEC', 'DZRD', 'DZRDP', 'EELT', 'ELFV', 'ENPG', 'ETLN', 'EUTR', 'FEES', 'FESH', 'FIVE',
                             'FIXP', 'FLOT', 'GAZA', 'GAZAP', 'GAZP', 'GCHE', 'GECO', 'GEMA', 'GLTR', 'GMKN', 'GTRK',
                             'HHRU', 'HIMCP', 'HNFG', 'HYDR', 'IGST', 'IGSTP', 'INGR', 'IRAO', 'IRKT', 'JNOS', 'JNOSP',
                             'KAZT', 'KAZTP', 'KBSB', 'KCHE', 'KCHEP', 'KGKC', 'KGKCP', 'KLSB', 'KMAZ', 'KMEZ', 'KOGK',
                             'KRKN', 'KRKNP', 'KRKOP', 'KROT', 'KROTP', 'KRSB', 'KRSBP', 'KTSB', 'KTSBP', 'KUBE',
                             'KUZB',
                             'KZOS', 'KZOSP', 'LENT', 'LIFE', 'LKOH', 'LNZL', 'LNZLP', 'LSNG', 'LSNGP', 'LSRG', 'LVHK',
                             'MAGE', 'MAGEP', 'MAGN', 'MDMG', 'MFGS', 'MFGSP', 'MGKL', 'MGNT', 'MGTS', 'MGTSP', 'MISB',
                             'MISBP', 'MOEX', 'MRKC', 'MRKK', 'MRKP', 'MRKS', 'MRKU', 'MRKV', 'MRKY', 'MRKZ', 'MRSB',
                             'MSNG', 'MSRS', 'MSTT', 'MTLR', 'MTLRP', 'MTSS', 'MVID', 'NAUK', 'NFAZ', 'NKHP', 'NKNC',
                             'NKNCP', 'NKSH', 'NLMK', 'NMTP', 'NNSB', 'NNSBP', 'NSVZ', 'NVTK', 'OGKB', 'OKEY', 'OMZZP',
                             'OZON', 'PAZA', 'PHOR', 'PIKK', 'PLZL', 'PMSB', 'PMSBP', 'POLY', 'POSI', 'PRFN', 'PRMB',
                             'QIWI', 'RASP', 'RBCM', 'RDRB', 'RENI', 'RGSS', 'RKKE', 'RNFT', 'ROLO', 'ROSB', 'ROSN',
                             'ROST', 'RTGZ', 'RTKM', 'RTKMP', 'RTSB', 'RTSBP', 'RUAL', 'RUSI', 'RZSB', 'SAGO', 'SAGOP',
                             'SARE', 'SAREP', 'SBER', 'SBERP', 'SELG', 'SFIN', 'SGZH', 'SIBN', 'SLEN', 'SMLT', 'SNGS',
                             'SNGSP', 'SOFL', 'SPBE', 'STSB', 'STSBP', 'SVAV', 'SVCB', 'SVET', 'TASB', 'TASBP', 'TATN',
                             'TATNP', 'TCSG', 'TGKA', 'TGKB', 'TGKBP', 'TGKN', 'TNSE', 'TORS', 'TORSP', 'TRMK', 'TRNFP',
                             'TTLK', 'TUZA', 'UGLD', 'UKUZ', 'UNAC', 'UNKL', 'UPRO', 'URKZ', 'USBN', 'UTAR', 'VEON-RX',
                             'VGSB', 'VGSBP', 'VJGZ', 'VJGZP', 'VKCO', 'VLHZ', 'VRSB', 'VRSBP', 'VSMO', 'VSYD', 'VSYDP',
                             'VTBR', 'WTCM', 'WTCMP', 'WUSH', 'YAKG', 'YKEN', 'YKENP', 'YNDX', 'YRSB', 'YRSBP', 'ZILL',
                             'ZVEZ']

    pairs_depth: list[str] = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "DOGEUSDT",
        "SHIBUSDT",
        "ADAUSDT"
]




settings = Settings()
