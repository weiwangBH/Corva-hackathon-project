import pydantic


class Settings(pydantic.BaseSettings):
    provider: str = "sample"
    version: int = 1

    wits_collection: str = "wits"
    drillstring_collection: str = "data.drillstring"
    output_collection: str = "calculated_bit_wear"


SETTINGS = Settings()
