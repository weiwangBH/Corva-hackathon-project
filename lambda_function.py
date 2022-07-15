from corva import Api, Cache, Logger, ScheduledDataTimeEvent, scheduled
from configuration import SETTINGS
import pandas as pd

ON_BOTTOM_ACTIVITIES = ["Rotary Drilling", "Slide Drilling"]


@scheduled
def lambda_handler(event: ScheduledDataTimeEvent, api: Api, cache: Cache):
    """Insert your logic here"""
    Logger.info('Baker Hughes Corva Hackathon app started ')

    # You have access to asset_id, start_time and end_time of the event.
    asset_id = event.asset_id
    company_id = event.company_id
    start_time = event.start_time
    end_time = event.end_time
    Logger.info(f'asset id:{asset_id}, start time: {start_time}, end time: {end_time}')

    # You have to fetch the realtime drilling data for the asset based on start and end time of the event.
    # start_time and end_time are inclusive so the query is structured accordingly to avoid processing duplicate data
    # We are only querying for weight_on_bit field since that is the only field we need. It is nested under data.
    wits_records = api.get_dataset(
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
        limit=3_600,
        fields="data.entry_at, data.weight_on_bit, data.rotary_rpm, data.state, metadata.drillstring"
    )

    # drillstring mongodb _id
    drillstring_id = wits_records[-1].get("metadata", {}).get("drillstring", None)

    if not drillstring_id:
        return

    drillstring_path = "v1/data/corva/data.drillstring/%s" % (drillstring_id)
    drillstring_response = api.get(path=drillstring_path)
    drillstring = drillstring_response.json()

    if not drillstring:
        return

    components = drillstring.get("data", {}).get("components", []) or []
    bit_size = next(
        (
            component.get("size")
            for component in reversed(components)
            if component.get("family") == "bit"
        ),
        None
    )

    if not bit_size:
        return

    wits_records = [
        record.get("data")
        for record in wits_records
    ]

    # remove non drilling states
    wits_records = [
        record
        for record in wits_records
        if record.get("state") in ON_BOTTOM_ACTIVITIES
    ]

    if not wits_records:
        return

    df = pd.DataFrame(wits_records)

    records_features = df

    last_wear = api.get_dataset(
        provider=SETTINGS.provider,
        dataset=SETTINGS.output_collection,
        query={
            'asset_id': asset_id
        },
        sort={'timestamp': -1},
        limit=1
    )

    wear_initial = 0
    if last_wear:
        last_wear = last_wear[0]
        last_wear.get("data", {}).get("cumulative_wear") or 0

    tau = 75
    H1 = 1.76
    H2 = 4.0
    W_d_max = 9.0
    h = 0.1 # for simplicity for now

    def wear_calculation(row) -> float:
        return (1 / tau) * pow(row['rotary_rpm'] / 60, H1) * \
            ((W_d_max - 4) / (W_d_max - row['weight_on_bit'] / bit_size)) * (1 + H2 * 0.5) / (1 + H2 * h) * \
            row['timestep']

    records_features['timestep'] = records_features['entry_at'].diff()
    records_features.loc[0, 'timestep'] = 1
    records_features['wear_rate'] = df.apply(wear_calculation, axis=1)
    records_total_wear = df['wear_rate'].sum() - records_features.loc[0, 'wear_rate']

    cumulative_wear = records_total_wear + wear_initial

    output = {
        "provider": SETTINGS.provider,
        "company_id": company_id,
        "collection": SETTINGS.output_collection,
        "asset_id": asset_id,
        "version": SETTINGS.version,
        "timestamp": end_time,
        "data": {
            "timestamp": end_time,
            "drilled_footage": None,
            "hole_depth": None,
            "drillstring_id": drillstring_id,
            "cumulative_wear": cumulative_wear,
        }
    }

    # Getting last exported timestamp from redis
    # last_exported_timestamp = int(cache.load(key='last_exported_timestamp') or 0)

    # if request fails, lambda will be re-invoked. so no exception handling
    api.post(
        f"api/v1/data/{SETTINGS.provider}/{SETTINGS.output_collection}/", data=[output],
    ).raise_for_status()

    # Storing the output timestamp to cache
    # cache.store(key='last_exported_timestamp', value=output.get("timestamp"))

    return output
