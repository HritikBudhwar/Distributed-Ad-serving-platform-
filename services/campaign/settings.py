from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    database_url: str = "postgresql://adpulse:adpulse@localhost:5432/adpulse"
    kafka_bootstrap: str = "localhost:19092"
    kafka_topic: str = "campaign.events"
    port: int = 8001


settings = Settings()
