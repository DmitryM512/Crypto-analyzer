#!/usr/bin/env python
import logging
import click
from apscheduler.schedulers.background import BlockingScheduler
from pytz import utc
from application.db import connect_db
from application.main import main
from application.parse_n_send import main_moex

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cli")


@click.group()
def cli() -> None:
    ...


@cli.command()
def run_scheduler() -> None:
    logger.info("Run scheduler")

    scheduler = BlockingScheduler(timezone=utc)

    with connect_db() as db_connection:
        scheduler.add_job(main, "cron", hour="*", minute=0, second=10, args=[db_connection, "1h"])
        scheduler.add_job(main, "cron", hour="*/4", minute=0, second=20, args=[db_connection, "4h"])
        scheduler.add_job(main, "cron", day="*", hour=0, minute=1, second=20, args=[db_connection, "1d"])
        scheduler.add_job(main_moex, "cron", day_of_week='mon-fri', hour=15, args=[db_connection, 24])

        scheduler.start()


@cli.command()
@click.option(
    "-t", "--time-frame", default='1d', help='Time frame', type=click.Choice(['1h', '4h', '1d'], case_sensitive=False)
)
def run_once(time_frame: str) -> None:
    logger.info("Run once")

    with connect_db() as db_connection:
        main(db_connection, time_frame)

@cli.command()
@click.option(
    "-i", "--interval", default=24, help='Interval', type=click.Choice([60, 24])
)
def run_once_moex(interval: int) -> None:
    logger.info('Run once MOEX')

    with connect_db() as db_connection:
        main_moex(db_connection, interval)


if __name__ == "__main__":
    cli()
