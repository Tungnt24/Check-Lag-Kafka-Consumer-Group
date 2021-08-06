import requests
from Kafka_Consumer_Group_Lag.setting import TeleBotConfig

def get_data():
	data = requests.get(TeleBotConfig.mqtt_url)
	data = data.json()[0].get(TeleBotConfig.mqtt_node)
	return data


def get_metrics(data):
	msg_in = data.get("msg_in")
	msg_out = data.get("msg_out")
	queue_in = data.get("queue_in")
	queue_out = data.get("queue_out")
	queue_drop = data.get("queue_drop")
	queue_unhandled = data.get("queue_unhandled")

	return {
		"message_in": msg_in,
		"message_out": msg_out,
		"queue_in": queue_in,
		"queue_out": queue_out,
		"queue_drop": queue_drop,
		"queue_unhandled": queue_unhandled
	}