import awswrangler as wr
import pandas as pd


def write_to_parquet(df: pd.DataFrame, bucket: str, path: str):
    """Write dataframe to bucket + path

    Args:
        df (pd.Dataframe): Dataframe to write
        bucket (str): S3 bucket
        path (str): Path to write to
    """
    wr.s3.to_parquet(
        df=df, path=f"s3://{bucket}/{path}", compression="snappy", index=False
    )
