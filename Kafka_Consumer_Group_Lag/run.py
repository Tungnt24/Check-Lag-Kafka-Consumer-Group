from Kafka_Consumer_Group_Lag.telegram_bot.lagbot import (
    telebot,
    get_data_kafka_group,
    get_lag
)
from Kafka_Consumer_Group_Lag.setting import TeleBotConfig
from Kafka_Consumer_Group_Lag.utils.logger import logger
import schedule
import time
import requests


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
    schedule.every(4).minutes.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
