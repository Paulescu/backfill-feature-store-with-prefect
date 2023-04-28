import os
from typing import List

# environment variables to connect to feature store
HOPSWORKS_PROJECT_NAME = os.environ['HOPSWORKS_PROJECT_NAME']
HOPSWORKS_API_KEY = os.environ['HOPSWORKS_API_KEY']

# feature group name and version
from dataclasses import dataclass
@dataclass
class FeatureGroupConfig:
    name: str
    version: int
    description: str
    primary_key: List[str]
    event_time: str


ohlc_feature_group_config = FeatureGroupConfig(
    name='ohlc_kraken_1min',
    version=1,
    description='OHLC data for Kraken BTC/EUR pair with 1 minute frequency',
    primary_key=['pair', 'ts'],
    event_time='ts'
)