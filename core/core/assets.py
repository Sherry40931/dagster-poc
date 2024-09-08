import polars as pl
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from sklearn import datasets


@asset(description="Fetch Iris dataset and save as parquet")
def fetch_data(context: AssetExecutionContext) -> MaterializeResult:
    df = datasets.load_iris(as_frame=True).frame
    df = pl.from_pandas(df)

    # save data
    path = "iris.parquet"
    df.write_parquet(path)

    # dagster logging
    context.log.info(f"Iris dataset shape: {df.shape}")

    return MaterializeResult(
        metadata={
            "row": MetadataValue.int(df.shape[0]),
            "column": MetadataValue.int(df.shape[1]),
            "preview": MetadataValue.md(df.head().to_pandas().to_markdown()),
            "filepath": MetadataValue.path(path),
        }
    )
