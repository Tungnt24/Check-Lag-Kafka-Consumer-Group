from dotenv import load_dotenv
import os

load_dotenv()


class TeleBotConfig:
    token = os.getenv("TELEBOT_TOKEN")
    api = os.getenv("TELE_API")
    chat_id = os.getenv("CHAT_ID")
    kowl_api = os.getenv("KOWL_API")
    consumer_group = os.getenv("CONSUMER_GROUP").split(",")
    heathcheck = os.getenv("HEATHCHECK")
