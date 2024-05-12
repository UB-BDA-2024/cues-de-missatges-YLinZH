import os

from pydantic import BaseSettings
from dotenv import load_dotenv
load_dotenv()

class Settings(BaseSettings):
    
    # 🔐 With an _ in front of the variable name, it is private, check the @property below
    _db_name: str = os.getenv("DB_NAME")
    db_user: str = os.getenv("DB_USER")
    db_password: str = os.getenv("DB_PASSWORD")
    db_host: str = os.getenv("DB_HOST")
    db_port: str = os.getenv("DB_PORT")
    
    @property
    def db_name(self) -> str:
        if (os.getenv("ENVIRONMENT") == "test"):
            return f"{self._db_name}_test"
        return self._db_name
    
    @db_name.setter
    def db_name(self, value: str) -> None:
        self._db_name = value
    
    @property
    def db_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"