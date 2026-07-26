import os
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

SOURCE_DATABASE_URL = os.environ["SOURCE_DATABASE_URL"]
TARGET_DATABASE_URL = os.environ["TARGET_DATABASE_URL"]
SOURCE_API_URL = os.environ["SOURCE_API_URL"]
SOURCE_DATA_START = datetime.fromisoformat(os.environ["SOURCE_DATA_START"])