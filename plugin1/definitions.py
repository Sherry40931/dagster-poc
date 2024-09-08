from datetime import datetime

from dagster import (
    AssetKey,
    Definitions,
    RunRequest,
    asset_sensor,
    define_asset_job,
    load_assets_from_modules,
)

from . import assets

all_assets = load_assets_from_modules([assets])

plugin1 = define_asset_job(name="plugin1", selection="random_forest_classifier")


@asset_sensor(
    asset_key=AssetKey("fetch_data"),
    description="Train a Random Forest classifier when data is materialized",
    job=plugin1,
)
def random_forest_classifier_sensor():
    return RunRequest(run_key=datetime.now())  # need to be different when triggered


defs = Definitions(
    assets=all_assets,
    sensors=[random_forest_classifier_sensor],
    jobs=[plugin1],
)
