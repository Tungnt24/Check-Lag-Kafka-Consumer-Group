from Kafka_Consumer_Group_Lag.setting import TeleBotConfig
import requests
import json


def get_data_kafka_group():
	res = requests.get(TeleBotConfig.kowl_api)
	if res.status_code != 200:
		return
	return res.content


def get_lag(data):
	result = []
	groups_data = json.loads(data)
	for key, value in groups_data.items():
		for group in value:
			if group['groupId'] in TeleBotConfig.consumer_group:
				for topic in group['lag']['topicLags']:
					if topic['summedLag'] >= 100:
						result.append(f"{group['groupId']} | total lag: {topic['summedLag']}")
	return result


def telebot(consumer_group_lag):
	api = TeleBotConfig.api
	text_format = '\n'.join(consumer_group_lag)
	for chat_id in TeleBotConfig.chat_ids:
		requests.post(api.format(TeleBotConfig.token, chat_id, f"``` {text_format} ```"))