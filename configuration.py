import pydantic


class Settings(pydantic.BaseSettings):
    provider: str
    output_collection: str = "bh-corva-project-scheduler-collection"
    wits_collection: str = "wits"
    version: int = 1


SETTINGS = Settings()
