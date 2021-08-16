from Kafka_Consumer_Group_Lag.telegram_bot.lagbot import (
    telebot,
    get_data_kafka_group,
    get_lag,
    telebot_mqtt
)
from Kafka_Consumer_Group_Lag.telegram_bot.vernemq import *
from Kafka_Consumer_Group_Lag.setting import TeleBotConfig
from Kafka_Consumer_Group_Lag.utils.logger import logger
import schedule
import time
import requests

def job_2():
    data = get_data()
    metrics = get_metrics(data)
    content = f"""
    MQTT:

    Message In: {metrics.get("message_in")}
    Message Out: {metrics.get("message_out")}
    Queue In: {metrics.get("queue_in")}
    Queue Out: {metrics.get("queue_out")}
    Queue Drop: {metrics.get("queue_drop")}
    Queue Unhandled: {metrics.get("queue_unhandled")}
    """
    if metrics.get("queue_in") - metrics.get("queue_out") >= 200:
        telebot_mqtt(content)
    try:
        requests.get(TeleBotConfig.heathcheck, timeout=20)
    except requests.RequestException as e:
        logger.info("Ping failed: %s" % e)

def job():
    data = get_data_kafka_group()
    consumer_group_lag = get_lag(data)
    if consumer_group_lag:
        telebot(consumer_group_lag)
    try:
        requests.get(TeleBotConfig.heathcheck, timeout=20)
    except requests.RequestException as e:
        logger.info("Ping failed: %s" % e)


if __name__ == "__main__":
    schedule.every(4).minutes.do(job_2)
    while True:
        schedule.run_pending()
        time.sleep(1)