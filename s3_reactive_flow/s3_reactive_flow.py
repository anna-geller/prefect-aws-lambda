import awswrangler as wr
import json
import logging
from prefect import flow, get_run_logger
from prefect.blocks.notifications import SlackWebhook


aws_logger = logging.getLogger()
aws_logger.setLevel(logging.INFO)


@flow
def validate_input_data(s3_key: str) -> None:
    logger = get_run_logger()
    logger.info("Received file: %s", s3_key)
    df = wr.s3.read_parquet(s3_key)
    max_value = max(df.value)
    if max_value > 42:
        alert = f"The max value {max_value} is bigger than 42! 🚨"
        logger.warning(alert)
        slack_webhook_block = SlackWebhook.load("hq")
        slack_webhook_block.notify(alert)
    else:
        logger.info("Data validation check passed ✅")


def handler(event, context):
    aws_logger.info("Received event: " + json.dumps(event))
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    s3_path = f"s3://{bucket}/{key}"
    validate_input_data(s3_path)
