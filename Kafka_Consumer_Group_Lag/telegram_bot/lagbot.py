from Kafka_Consumer_Group_Lag.setting import TeleBotConfig
from Kafka_Consumer_Group_Lag.utils.logger import logger
import requests
import json


def get_data_kafka_group():
    res = requests.get(TeleBotConfig.kowl_api)
    if res.status_code != 200:
        logger.error(f"requests to kowl api | status code {res.status_code}")
        return
    return res.content


def get_lag(data):
    result = []
    total_lag = []
    groups_data = json.loads(data)
    for key, value in groups_data.items():
        for group in value:
            if group["groupId"] in TeleBotConfig.consumer_group:
                logger.info(f'GROUP NAME: {group["groupId"]}')
                for topic in group["topicOffsets"]:
                    if topic["summedLag"] >= 100:
                        total_lag.append(topic['summedLag'])
                if sum(total_lag) >= 100:
                    result.append(
                        f"{group['groupId']} | total lag: {sum(total_lag)}"
                    )
                    total_lag.clear()
    return result


def telebot(consumer_group_lag):
    api = TeleBotConfig.api
    text_format = "\n".join(consumer_group_lag)
    logger.info(f"SENDING MESSAGE: {text_format}")
    requests.post(
        api.format(
            TeleBotConfig.token,
            TeleBotConfig.chat_id,
            f"``` {text_format} ```",
        )
    )

def telebot_mqtt(content):
    api = TeleBotConfig.api
    requests.post(api.format(TeleBotConfig.token, TeleBotConfig.chat_id, f"{content}"))