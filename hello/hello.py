from prefect import task, flow, get_run_logger


@task
def say_hi(user_name: str):
    logger = get_run_logger()
    logger.info("Hello from AWS Lambda, %s! 🦆", user_name)


@flow
def hello(user: str = "Marvin"):
    say_hi(user)


def handler(event, context):
    hello()
