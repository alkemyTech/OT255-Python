import os
from pathlib import Path

from dotenv import load_dotenv

# Variables .env
path_env = Path(__file__).parent.parent.parent / ".gitignore" / ".env"
dotenv_path = path_env
load_dotenv(dotenv_path)
host_db = os.getenv("DB_HOST")
user_db = os.getenv("DB_USER")
password_db = os.getenv("DB_PASS")
database_db = os.getenv("DB_NAME")
port_db = os.getenv("DB_PORT")
