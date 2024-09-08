import polars as pl
from dagster import asset, get_dagster_logger
from sklearn.ensemble import RandomForestClassifier


@asset(description="Train a Random Forest classifier")
def random_forest_classifier() -> RandomForestClassifier:
    # polars to pandas
    df = pl.read_parquet("iris.parquet").to_pandas()

    # train model
    model = RandomForestClassifier(random_state=0)
    x, y = df.drop(columns=["target"]), df["target"]
    model.fit(x, y)

    # dagster logging
    get_dagster_logger().info("Random forest model trained successfully")

    return model
