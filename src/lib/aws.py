import pandas as pd


def write_to_parquet(
    df: pd.DataFrame, bucket: str, path: str, partitions: list[str] = []
):
    """Write dataframe to bucket + path

    Args:
        df (pd.Dataframe): Dataframe to write
        bucket (str): S3 bucket
        path (str): Path to write to
    """

    df.to_parquet(
        path=f"s3://{bucket}/{path}",
        compression="snappy",
        index=False,
        partition_cols=partitions,
    )
