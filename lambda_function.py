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
        fields="data.bit_depth, data.block_height, data.hole_depth, data.hook_load, data.rotary_rpm, data.standpipe_pressure"
        
    )

    record_count = len(records)
    
    pipeline_onnx_file="C:/Users/wangwei11/Documents/projects/repos/CodeAAA/code/modeling/xgboost-modeling/xgboost_modeling/codes/new data/model_outputs/pipeline_xgboost_63data_v2.onnx"
    xgb_model_onnx_file="C:/Users/wangwei11/Documents/projects/repos/CodeAAA/code/modeling/xgboost-modeling/xgboost_modeling/codes/new data/model_outputs/xgboost_model_final_63data_v2.onnx"

    
    FACTOR_FIELDS_xgb = ['DEPTH_FLAG', 'DELTA_BDEPTH_FLAG', 'RPM_FLAG', 'SPP_FLAG', 'HKLI_group_0', 'HKLI_group_1', 'HKLI_group_2', 'rig_state_level1', 'HKHT_group_0', 'HKHT_group_1', 'HKHT_group_2']
    CONTINUOUS_FIELDS_xgb = ['PCT_DEPTH', 'BIT_DEPTH_RATE_p30s', 'DELTA_BDEPTH', 'DELTA_BDEPTH_30s', 'DELTA_DEPTH_30s',\
                         'DELTA_HKHT_30s', 'DELTA_HKLI_30s', 'DEPTH_DIFF', 'HKHT', 'HKHT_RATE_p30s', 'HKHT_p30s', 'HKLI',\
                         'HKLI_RATE_p30s', 'HKLI_p30s', 'HOLE_DEPTH_RATE_p30s', 'PREV_BIT_DEPTH', 'RPM', 'SPP','HKLI_p60s',\
                         'HKHT_p60s', 'DELTA_BDEPTH_60s', 'DELTA_DEPTH_60s', 'DELTA_HKLI_60s', 'DELTA_HKHT_60s', 'BIT_DEPTH_RATE_p60s',\
                         'HOLE_DEPTH_RATE_p60s', 'HKLI_RATE_p60s', 'HKHT_RATE_p60s']
   
    records=records.sort_values(by=['timestamp']).reset_index(drop=True)
    for input_col in ['bit_depth', 'block_height', 'hole_depth', 'hook_load', 'rotary_rpm', 'standpipe_pressure']:
        records[input_col]=np.where(records[input_col]==-999.25, np.nan, records[input_col])
    records['TIME'] = records['time_ms'].apply(lambda x: datetime.fromtimestamp(float(x)))
    records['TIME'] = records['TIME'].dt.tz_localize(None)
    
    data=records.copy()
    data=data.rename(columns={'bit_depth': 'BITDEPTH', 'block_height': 'HKHT', 'hole_depth': 'DEPTH', 'hook_load': 'HKLI', 'rotary_rpm': 'RPM', 'standpipe_pressure': 'SPP'})
    data['prev_TIME']=data['TIME']
    data_time_shift=data.prev_TIME.shift(1)
    data=pd.concat([data.drop(['prev_TIME'], axis=1), data_time_shift], axis=1)
    data['time_diff']=(data['TIME']-data['prev_TIME']).dt.seconds
    data['PREV_BIT_DEPTH']=data['BITDEPTH'].shift()
    data['DELTA_BDEPTH'] = data['BITDEPTH'] - data['PREV_BIT_DEPTH']
    data['DELTA_BDEPTH_FLAG'] = np.where(np.abs(data['DELTA_BDEPTH']) > 0.005, 1, data['DELTA_BDEPTH']) # pipe moving or not
    data['DEPTH_DIFF'] = data['DEPTH'] - data['BITDEPTH']
    data['DEPTH_FLAG'] = np.where(data['DEPTH_DIFF'] > 0.33, 1, 0) # on/off bottom
    data['DEPTH_FLAG'] = np.where(pd.isnull(data['DEPTH_DIFF']), np.nan, data['DEPTH_FLAG'])
    data['RPM_FLAG'] = np.where(data['RPM'] > 20, 1, 0) # rpm>20 - rotating
    data['RPM_FLAG'] = np.where(pd.isnull(data['RPM']), np.nan, data['RPM_FLAG'])
    data['SPP_FLAG'] = np.where(data['SPP'] > 200, 1, 0) # SPP>200 psi - circulating
    data['SPP_FLAG'] = np.where(pd.isnull(data['SPP']), np.nan, data['SPP_FLAG'])
    data['PCT_DEPTH']=data['BITDEPTH']/data['DEPTH']
    data['HKLI_group_0']=np.where((data['HKLI']<120), 1, 0)
    data['HKLI_group_0'] = np.where(pd.isnull(data['HKLI']), np.nan, data['HKLI_group_0'])
    data['HKLI_group_1']=np.where(((data['HKLI']>=120)&(data['HKLI']<250)), 1, 0)
    data['HKLI_group_1'] = np.where(pd.isnull(data['HKLI']), np.nan, data['HKLI_group_1'])
    data['HKLI_group_2']=np.where((data['HKLI']>=250), 1, 0)
    data['HKLI_group_2'] = np.where(pd.isnull(data['HKLI']), np.nan, data['HKLI_group_2'])
    data['HKHT_group_0']=np.where((data['HKHT']<40), 1, 0)
    data['HKHT_group_0'] = np.where(pd.isnull(data['HKHT']), np.nan, data['HKHT_group_0'])
    data['HKHT_group_1']=np.where(((data['HKHT']>=40)&(data['HKHT']<100)), 1, 0)
    data['HKHT_group_1'] = np.where(pd.isnull(data['HKHT']), np.nan, data['HKHT_group_1'])
    data['HKHT_group_2']=np.where((data['HKHT']>=100), 1, 0)
    data['HKHT_group_2'] = np.where(pd.isnull(data['HKHT']), np.nan, data['HKHT_group_2'])
    
    data['prior_30s_TIME']=data['TIME']-pd.Timedelta(seconds=30)
    data=data.merge(data[['TIME', 'BITDEPTH', 'DEPTH', 'HKLI', 'HKHT']].rename(columns={'BITDEPTH': 'BITDEPTH_p30s',\
                'DEPTH': 'DEPTH_p30s', 'HKLI': 'HKLI_p30s', 'HKHT': 'HKHT_p30s', 'TIME': 'prior_30s_TIME'}),\
                on=['prior_30s_TIME'], how='left')
    data['DELTA_BDEPTH_30s']=data['BITDEPTH']-data['BITDEPTH_p30s']
    data['DELTA_DEPTH_30s']=data['DEPTH']-data['DEPTH_p30s']
    data['DELTA_HKLI_30s']=data['HKLI']-data['HKLI_p30s']
    data['DELTA_HKHT_30s']=data['HKHT']-data['HKHT_p30s']
    data['BIT_DEPTH_RATE_p30s']=np.where((pd.notnull(data['BITDEPTH_p30s'])), data['DELTA_BDEPTH_30s']/30, np.nan)
    data['HOLE_DEPTH_RATE_p30s']=np.where((pd.notnull(data['DEPTH_p30s'])), data['DELTA_DEPTH_30s']/30, np.nan)
    data['HKLI_RATE_p30s']=np.where((pd.notnull(data['HKLI_p30s'])), data['DELTA_HKLI_30s']/30, np.nan)
    data['HKHT_RATE_p30s']=np.where((pd.notnull(data['HKHT_p30s'])), data['DELTA_HKHT_30s']/30, np.nan)
    
    data['prior_60s_TIME']=data['TIME']-pd.Timedelta(seconds=60)
    data=data.merge(data[['TIME', 'BITDEPTH', 'DEPTH', 'HKLI', 'HKHT']].rename(columns={'BITDEPTH': 'BITDEPTH_p60s',\
                'DEPTH': 'DEPTH_p60s', 'HKLI': 'HKLI_p60s', 'HKHT': 'HKHT_p60s', 'TIME': 'prior_60s_TIME'}), on=['prior_60s_TIME'], how='left')
    data['DELTA_BDEPTH_60s']=data['BITDEPTH']-data['BITDEPTH_p60s']
    data['DELTA_DEPTH_60s']=data['DEPTH']-data['DEPTH_p60s']
    data['DELTA_HKLI_60s']=data['HKLI']-data['HKLI_p60s']
    data['DELTA_HKHT_60s']=data['HKHT']-data['HKHT_p60s']
    data['BIT_DEPTH_RATE_p60s']=np.where((pd.notnull(data['BITDEPTH_p60s'])), data['DELTA_BDEPTH_60s']/60, np.nan)
    data['HOLE_DEPTH_RATE_p60s']=np.where((pd.notnull(data['DEPTH_p60s'])), data['DELTA_DEPTH_60s']/60, np.nan)
    data['HKLI_RATE_p60s']=np.where((pd.notnull(data['HKLI_p60s'])), data['DELTA_HKLI_60s']/60, np.nan)
    data['HKHT_RATE_p60s']=np.where((pd.notnull(data['HKHT_p60s'])), data['DELTA_HKHT_60s']/60, np.nan)
    
    data['SPP']=data['SPP'].astype(float)
    data['rig_state_level1']=np.where(data['DEPTH_DIFF']<100, 1, 0)
    data['rig_state_level1'] = np.where(pd.isnull(data['DEPTH_DIFF']), np.nan, data['rig_state_level1'])
        
    data2=data.copy()
    sess = rt.InferenceSession(pipeline_onnx_file)
    pred_pipe_onnx = sess.run(None, {"input": np.array(data2[CONTINUOUS_FIELDS_xgb]).astype(np.float32)})
    data_t = pd.concat([pd.DataFrame(pred_pipe_onnx[0], columns=CONTINUOUS_FIELDS_xgb), data2[FACTOR_FIELDS_xgb]], axis=1)
    data_t = data_t.apply(lambda x: np.nan_to_num(x, nan=-999.25))
    i=0
    for col in data_t.columns.tolist():
        data_t.rename(columns={col:'f'+str(i)}, inplace=True)
        i=i+1
    sess = rt.InferenceSession(xgb_model_onnx_file)
    pred_model_onnx = sess.run(None, {"input": np.array(data_t).astype(np.float32)})
    data_predict_xgb=pd.DataFrame(pred_model_onnx[0]).rename(columns={0:'ra_code3'}) 
    data_predict_xgb['ra_desc3']=data_predict_xgb['ra_code3'].map(state_decoding_dict_xgb)
    data_predict_xgb=pd.concat([data2[['rig_state_level1', 'BITDEPTH', 'SPP']], data_predict_xgb], axis=1)
    data_predict_xgb['rig_state_level1']=np.where(data_predict_xgb['BITDEPTH']<90, 2, data_predict_xgb['rig_state_level1'])
    data_predict_xgb['rig_state_level1']=np.where((data_predict_xgb['rig_state_level1']==0)&(data_predict_xgb['BITDEPTH']<1000), 3, data_predict_xgb['rig_state_level1'])
    data_predict_xgb['rig_state_level1']=data_predict_xgb['rig_state_level1'].fillna(-1)
    data_predict_xgb['rig_state_level1']=data_predict_xgb['rig_state_level1'].astype(int)
    data_predict_xgb['rig_state_level2']=np.where((data_predict_xgb['ra_desc3']=='Rotary Drilling')|(data_predict_xgb['ra_desc3']=='Sliding')|\
                                                        (data_predict_xgb['ra_desc3']=='Non-drilling'), 0, 1)
    data_predict_xgb['rig_state_level2']=np.where(data_predict_xgb['rig_state_level1']==2, -1, data_predict_xgb['rig_state_level2'])
    data_predict_xgb['rig_state_level2']=np.where(data_predict_xgb['rig_state_level1']==-1, 2, data_predict_xgb['rig_state_level2'])
    data_predict_xgb['ra_desc3']=np.where((data_predict_xgb['SPP']<200)&(data_predict_xgb['ra_desc3']=='Ream up'), 'Rotation up', data_predict_xgb['ra_desc3'])
    data_predict_xgb['ra_desc3']=np.where((data_predict_xgb['SPP']<200)&(data_predict_xgb['ra_desc3']=='Ream down'), 'Rotation down', data_predict_xgb['ra_desc3'])
    data_predict_xgb['ra_desc3']=np.where((data_predict_xgb['SPP']<200)&(data_predict_xgb['ra_desc3']=='Rotation + Circulation'), 'Rotation', data_predict_xgb['ra_desc3'])
    data_predict_xgb['rig_state_level3']=data_predict_xgb['ra_desc3'].map(inverted_level3_state_dict)
    data_predict_xgb['rig_state_level3']=np.where(data_predict_xgb['rig_state_level1']==2, -1, data_predict_xgb['rig_state_level3'])
    data_predict_xgb['ra_code3']=data_predict_xgb.apply(lambda x: str(x['rig_state_level1'])+str(x['rig_state_level2'])+str(x['rig_state_level3']).zfill(2), axis=1)
    data_predict_xgb['ra_desc3']=data_predict_xgb.apply(lambda x: level1_state_dict[x['rig_state_level1']]+'_'+level2_state_dict[x['rig_state_level2']]+'_'+level3_state_dict[x['rig_state_level3']], axis=1)
    data_t_prob=pd.DataFrame(pred_model_onnx[1])
    data2['cl_code3']='DD_XGBoost'
    data_t2=pd.concat([data2, data_predict_xgb[['ra_code3', 'ra_desc3', 'rig_state_level1', 'rig_state_level2', 'rig_state_level3']].\
                       rename(columns={'rig_state_level1': 'ra_code3_l1', 'rig_state_level2': 'ra_code3_l2', 'rig_state_level3': 'ra_code3_l3'})], axis=1)
    data_t2['ra_desc3_l1']=data_t2['ra_code3_l1'].map(level1_state_dict)
    data_t2['ra_desc3_l2']=data_t2['ra_code3_l2'].map(level2_state_dict)
    data_t2['ra_desc3_l3']=data_t2['ra_code3_l3'].map(level3_state_dict)
    
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
