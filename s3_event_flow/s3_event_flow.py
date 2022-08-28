import awswrangler as wr
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from typing import Union
from prefect import flow, get_run_logger
from prefect.blocks.system import JSON


class TimeseriesGenerator:
    def __init__(
        self,
        start_date: Union[str, datetime] = datetime.today(),
        end_date: Union[str, datetime] = datetime.today() + timedelta(days=7),
        frequency: str = "H",
        dt_column: str = "timestamp",
        nr_column: str = "value",
        min_value: int = 0,
        max_value: int = 42,
    ) -> None:
        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.dt_column = dt_column
        self.nr_column = nr_column
        self.min_value = min_value
        self.max_value = max_value

    def get_date_range(self) -> pd.date_range:
        return pd.date_range(
            start=self.start_date, end=self.end_date, freq=self.frequency
        )

    @classmethod
    def get_timeseries(cls, **kwargs) -> pd.DataFrame:
        ts = cls(**kwargs)
        timestamp_date_range = ts.get_date_range()
        timeseries_df = pd.DataFrame(timestamp_date_range, columns=[ts.dt_column])
        timeseries_df[ts.nr_column] = np.random.randint(
            ts.min_value, ts.max_value, size=len(timestamp_date_range)
        )
        return timeseries_df


@flow
def upload_timeseries_data_to_s3() -> None:
    dict_from_block = JSON.load("max-value").value
    max_val = dict_from_block["threshold"]
    df = TimeseriesGenerator.get_timeseries(max_value=max_val)
    result = wr.s3.to_parquet(
        df,
        path=f"s3://prefectdata/timeseries/",
        index=False,
        dataset=True,
        database="default",
        table="lambda",
    )
    logger = get_run_logger()
    logger.info("New file uploaded: %s 🚀", result.get("paths")[0])


def handler(event, context):
    upload_timeseries_data_to_s3()
