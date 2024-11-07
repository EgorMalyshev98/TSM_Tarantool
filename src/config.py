from dataclasses import dataclass
from os import environ as env

from dotenv import load_dotenv

load_dotenv()

SECRET_AUTH = env.get("SECRET_AUTH")
DEV = env.get("DEV")


@dataclass(frozen=True)
class DBConfig:
    DB_USER = env.get("DB_USER")
    DB_PASS = env.get("DB_PASS")
    DB_HOST = env.get("DB_HOST")
    DB_PORT = env.get("DB_PORT")
    DB_NAME = env.get("DB_NAME")

    # ROOT_PATH = env.get("DB_NAME")

    def get_db_url(self):
        return f"postgresql+psycopg2://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    def get_async_db_url(self):
        return f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PASS}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"


db_config = DBConfig()
