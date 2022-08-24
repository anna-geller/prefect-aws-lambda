import awswrangler as wr
from datetime import datetime
import pandas as pd
from prefect import task, flow, get_run_logger
import requests


@task
def extract_current_prices():
    url = "https://min-api.cryptocompare.com/data/pricemulti?fsyms=BTC,ETH,REP,DASH&tsyms=USD"
    r = requests.get(url)
    return r.json()


@task
def transform_current_prices(json_data: dict) -> pd.DataFrame:
    df = pd.DataFrame(json_data)
    df["TIME"] = datetime.utcnow()
    return df.reset_index(drop=True)


@task
def load_current_prices(df: pd.DataFrame):
    wr.s3.to_parquet(
        df=df,
        path="s3://prefectdata/crypto/",
        dataset=True,
        mode="append",
        database="default",
        table="crypto",
    )
    logger = get_run_logger()
    logger.info("Data loaded to a data lake! 🎉")


@flow
def crypto_prices_etl():
    raw_data = extract_current_prices()
    data = transform_current_prices(raw_data)
    load_current_prices(data)


def handler(event, context):
    crypto_prices_etl()
