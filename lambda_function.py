from corva import Api, Cache, Logger, ScheduledDataTimeEvent, scheduled


@scheduled
def lambda_handler(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('Hello, World!')
