from corva import Api, Cache, Logger, ScheduledDataTimeEvent, scheduled
from configuration import SETTINGS

@scheduled
def lambda_handler(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('Baker Hughes Corva Hackathon app started ')

    # You have access to asset_id, start_time and end_time of the event.
    asset_id = event.asset_id
    start_time = event.start_time
    end_time = event.end_time

    # You have to fetch the realtime drilling data for the asset based on start and end time of the event.
    # start_time and end_time are inclusive so the query is structured accordingly to avoid processing duplicate data
    # We are only querying for weight_on_bit field since that is the only field we need. It is nested under data.
    records = api.get_dataset(
        provider="corva",
        dataset=SETTINGS.wits_collection,
        query={
            'asset_id': asset_id,
            'timestamp': {
                '$gte': start_time,
                '$lte': end_time,
            }
        },
        sort={'timestamp': 1},
        limit=500,
        fields="data.bit_depth", "data.block_height", "data.hole_depth", "data.hook_load", "data.rotary_rpm", "data.standpipe_pressure"
        
    )

    record_count = len(records)

    # Computing mean weight on bit from the list of realtime wits records
    # mean_weight_on_bit = statistics.mean(record.get("data", {}).get("weight_on_bit", 0) for record in records)
    # TODO model prediction here
    company_id = records[0].get("company_id")

    # Getting last exported timestamp from redis
    last_exported_timestamp = int(cache.load(key='last_exported_timestamp') or 0)

    # Making sure we are not processing duplicate data
    if end_time <= last_exported_timestamp:
        Logger.debug(f"Already processed data until {last_exported_timestamp=}")
        return None

    # Building the required output
    output = {
        "timestamp": end_time,
        "asset_id": asset_id,
        "company_id": company_id,
        "provider": SETTINGS.provider,
        "collection": SETTINGS.output_collection,
        "data": {
            "rig_state_prediction": ,
            "start_time": start_time,
            "end_time": end_time
        },
        "version": SETTINGS.version
    }

    Logger.debug(f"{asset_id=} {company_id=}")
    Logger.debug(f"{start_time=} {end_time=} {record_count=}")
    Logger.debug(f"{output=}")

    # if request fails, lambda will be re-invoked. so no exception handling
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=[output],
    ).raise_for_status()

    # Storing the output timestamp to cache
    cache.store(key='last_exported_timestamp', value=output.get("timestamp"))

    return output
