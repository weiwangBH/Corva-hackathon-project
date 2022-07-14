from corva import Api, Cache, Logger, ScheduledNaturalTimeEvent, scheduled
from configuration import SETTINGS

@scheduled
def lambda_handler(event: ScheduledNaturalTimeEvent, api: Api, cache: Cache):
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
        fields="timestamp, data.bha_id, data.weight_on_bit, data.rotary_rpm, data.state"
        
    )
    Logger.info(f'asset id:{asset_id}, start time: {start_time}, end time: {end_time}')
    record_count = len(records)
    
    records=pd.json_normalize(a).drop(['_id'], axis=1)
    records=records[-1:].rename(columns={'data.weight_on_bit': 'weight_on_bit', 'data.rotary_rpm': 'rotary_rpm'})
    
    tau = 75
    H1 = 1.76
    H2 = 4.0
    W_d_max = 9.0
    h=0
    
    try:
        if records.get("state")=='Slide Drilling' | 'Rotary Drilling':
            if records.timestamp=0:
                h=0
            else:
                bit_wear_rate = (1/tau) * pow(RPM/60,H1)*((W_d_max - 4)/(W_d_max - WOB/bit_size))*(1 + H2*0.5)/(1 + H2*h)
                h+=bit_wear_rate*delta_time/3600
        else:
            bit_wear_rate=0
    except:
        Logger.info('Oopss cannot run the calcs')
    
    # TODO model prediction here
    company_id = records.get("company_id")

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
            "new_wear_state": ,
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
