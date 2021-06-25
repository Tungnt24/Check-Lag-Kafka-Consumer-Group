from Kafka_Consumer_Group_Lag.telegram_bot.lagbot import (
    telebot,
    get_data_kafka_group,
    get_lag,
)
import schedule
import time


def job():
    data = get_data_kafka_group()
    consumer_group_lag = get_lag(data)
    telebot(consumer_group_lag)


if __name__ == "__main__":
    schedule.every(30).seconds.do(job)
    while True:
        schedule.run_pending()
        time.sleep(1)
