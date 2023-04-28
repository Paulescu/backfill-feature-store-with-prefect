from typing import Optional

import hsfs
import hopsworks
import pandas as pd
from prefect import task

from src.config import FeatureGroupConfig
import src.config as config

def get_feature_store() -> hsfs.feature_store.FeatureStore:
    """Connects to Hopsworks and returns a pointer to the feature store

    Returns:
        hsfs.feature_store.FeatureStore: pointer to the feature store
    """
    project = hopsworks.login(
        project=config.HOPSWORKS_PROJECT_NAME,
        api_key_value=config.HOPSWORKS_API_KEY
    )
    return project.get_feature_store()

def get_or_create_feature_group(
    feature_group_config: FeatureGroupConfig
    ) -> hsfs.feature_group.FeatureGroup:
    """Connects to the feature store and returns a pointer to the given
    feature group `name`

    Args:
        name (str): name of the feature group
        version (Optional[int], optional): _description_. Defaults to 1.

    Returns:
        hsfs.feature_group.FeatureGroup: pointer to the feature group
    """
    return get_feature_store().get_or_create_feature_group(
        name=feature_group_config.name,
        version=feature_group_config.version,
        description=feature_group_config.description,
        primary_key=feature_group_config.primary_key,
        event_time=feature_group_config.event_time
    )

@task(retries=3, retry_delay_seconds=60)
def save_data_to_feature_group(
    data: pd.DataFrame,
    feature_group_config: FeatureGroupConfig
) -> None:
    """"""
    feature_group = get_or_create_feature_group(feature_group_config)
    feature_group.insert(data, write_options={"wait_for_job": False})

