from datetime import datetime, timedelta, date
from argparse import ArgumentParser
from typing import List

import pytz
import requests
import pandas as pd
from prefect import task, flow
from prefect.task_runners import SequentialTaskRunner

from src.feature_store_api import save_data_to_feature_group
from src import config
from src.logger import get_logger

logger = get_logger()

PAIR = 'xbtusd'
OHLC_FREQUENCY_IN_MINUTES = 1

@task(retries=3, retry_delay_seconds=60)
def fetch_data_from_kraken_api(
    pair: str,
    since_nano_seconds: int
) -> List[List]:
    """
    Fetches data from Kraken API for the given `pair` from `since_nano_seconds`

    Args:
        pair (str): currency pair we fetch data for
        last_ts (int): timestamp in seconds

    Returns:
        dict: response from Kraken API
    """
    # build URL we need to fetch data from
    url = f"https://api.kraken.com/0/public/Trades?pair={PAIR}&since={since_nano_seconds:.0f}"

    # fetch data
    response = requests.get(url)

    # extract data from response
    # TODO: update currency ticker
    trade_params = response.json()["result"]['XXBTZUSD']

    return trade_params


@flow
def fetch_historical_data_one_day(day: datetime) -> pd.DataFrame:
    """"""
    # time range we want to fetch data for
    from_ts = day.timestamp()
    to_ts = (day + timedelta(days=1)).timestamp()
    # to_ts = (day + timedelta(hours=1)).timestamp()

    trades = []
    last_ts = from_ts
    while last_ts < to_ts:  
        
        # fetch data from Kraken API
        trade_params = fetch_data_from_kraken_api(
            PAIR,
            since_nano_seconds=last_ts*1000000000)

        # create a list of Dict with the results
        trades_in_batch = [
            {'price': params[0], 'volume': params[1], 'ts': params[2]}
            for params in trade_params
        ]

        # checking timestamps
        from_date = datetime.fromtimestamp(trades_in_batch[0]['ts'])
        to_date = datetime.fromtimestamp(trades_in_batch[-1]['ts'])
        logger.info(f'trades_in_batch from {from_date} to {to_date}')

        # add trades to list of trades
        trades += trades_in_batch

        # update last_ts for the next iteration
        last_ts = trades[-1]['ts']

    # drop trades that might fall outside of the time window
    trades = [t for t in trades if t['ts'] >= from_ts and t['ts'] <= to_ts]
    logger.info(f'Fetched trade data from {trades[0]["ts"]} to {trades[-1]["ts"]}')
    logger.info(f'{len(trades)=}')

    # convert trades to pandas dataframe
    trades = pd.DataFrame(trades)

    # set correct dtypes
    trades['price'] = trades['price'].astype(float)
    trades['volume'] = trades['volume'].astype(float)
    trades['ts'] = trades['ts'].astype(int)

    return trades

@task
def transform_trades_to_ohlc(trades: pd.DataFrame) -> pd.DataFrame:
    """Transforms raw trades to OHLC data"""
    
    # convert ts column to pd.Timestamp
    trades.index = pd.to_datetime(trades['ts'], unit='s')

    # convert trade data to OHLC data
    logger.info('Converting raw trades to OHLC data')
    ohlc = trades['price'] \
        .resample(f'{OHLC_FREQUENCY_IN_MINUTES}Min') \
        .ohlc()

    # extract index with `ts` as column
    ohlc.reset_index(inplace=True)

    return ohlc

@flow(task_runner=SequentialTaskRunner())
def backfill_one_day(day: datetime):
    """Backfills OHLC data in the feature store for the given `day`"""
    
    # fetch trade data from external API
    trades: pd.DataFrame = fetch_historical_data_one_day(day)

    # transform trade data to OHLC data
    ohlc: pd.DataFrame = transform_trades_to_ohlc(trades)

    # push OHLC data to the feature store
    logger.info('Pushing OHLC data to the feature store')
    save_data_to_feature_group(
        ohlc, feature_group_config=config.ohlc_feature_group_config)

@flow
def backfill_range_dates(from_day: datetime, to_day: datetime):
    """
    Backfills OHLC data in the feature store for the ranges of days
    between `from_day` and `to_day`
    """
    days = pd.date_range(from_day, to_day, freq='D')
    for day in days:
        backfill_one_day(day)


if __name__ == "__main__":
    
    parser = ArgumentParser()
    parser.add_argument(
        '--from_day',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    )
    parser.add_argument(
        '--to_day',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    )
    parser.add_argument(
        '--day',
        type=lambda s: datetime.strptime(s, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    )

    args = parser.parse_args()

    if args.day:
        logger.info(f'Starting backfilling process for {args.day}')
        backfill_one_day(day=args.day)
    
    else:
        assert args.from_day < args.to_day, "from_day has to be smaller than to_day"
        logger.info(f'Starting backfilling process from {args.from_day} to {args.to_day}')
        backfill_range_dates(from_day=args.from_day, to_day=args.to_day)
