from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(retries=3)
def extract_from_gcs(param) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{param}_stats_data.csv"
    gcs_block = GcsBucket.load("ugm-bucket")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"{gcs_path}")

@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_csv(path, compression='gzip')
    return df

@task()
def write_bq(df: pd.DataFrame, param) -> None:
    """Write DataFrame to BiqQuery"""

    df.to_gbq(
        destination_table=f"youtube_data_all.{param}_stats",
        project_id="ugm-yt-etl",
        chunksize=100,
        if_exists="replace",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""

    path = extract_from_gcs('channel')
    df = transform(path)
    write_bq(df, 'channel')

    path = extract_from_gcs('videos')
    df = transform(path)
    write_bq(df, 'videos')

if __name__ == "__main__":
    etl_gcs_to_bq()
