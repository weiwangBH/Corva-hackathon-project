import pydantic


class Settings(pydantic.BaseSettings):
    provider: str
    output_collection: str = "BH-Corva-project-scheduler-collection"
    wits_collection: str = "wits"
    version: int = 1


SETTINGS = Settings()
