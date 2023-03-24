from pydantic import BaseSettings


class Settings(BaseSettings):
    postgres_db: str
    postgres_host: str
    postgres_port: str
    postgres_user: str
    postgres_password: str
    redis_host: str

    class Config:
        env_file = ".env"


settings = Settings()
