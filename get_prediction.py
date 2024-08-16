import logging
import os
import pandas as pd
import joblib
from pymongo import MongoClient
import gridfs
import io


logger = logging.getLogger(__name__)


m_string = os.getenv('m_string')


def data_prep(df, trained_features):
    logger.info('Preparing data for prediction')
    required_columns = ['perfumer', 'brand_name', 'notes', 'accords', 'perfume_id']
    if not all(col in df.columns for col in required_columns):
        raise ValueError(f"Input DataFrame must contain the following columns: {required_columns}")
    df['desc'] = (df['perfumer'] + ',' + df['brand_name'] + ',' + df['notes'] + ',' + df['accords']) \
        .str.lower().str.replace('[()-]', ' ', regex=True).str.split(',')
    perfume_info = df[['perfume_id', 'desc']].explode('desc')
    perfume_info = pd.get_dummies(perfume_info, columns=['desc'], prefix='', prefix_sep='')
    perfume_info = perfume_info.groupby('perfume_id', as_index=False).sum()
    perfume_info = perfume_info.loc[:, (perfume_info != 0).any(axis=0)]
    missing_features = list(set(trained_features) - set(perfume_info.columns))
    if missing_features:
        missing_df = pd.DataFrame(0, index=perfume_info.index, columns=missing_features)
        perfume_info = pd.concat([perfume_info, missing_df], axis=1)
    perfume_info = perfume_info[trained_features]
    logger.info('Data for prediction is ready')
    return perfume_info


def get_prediction(new_data):
    logger.info('Getting prediction')
    client = MongoClient(m_string)
    db = client['scent_db']
    collection = db['training_features']
    retrieved_document = collection.find_one({'list_name': 'scent_train'})
    trained_features = retrieved_document['list_data'] if retrieved_document else None

    def get_model(model_name,db):
        fs = gridfs.GridFS(db)
        file_data = fs.find_one({'filename': f'{model_name}.pkl'})
        model_bytes = io.BytesIO(file_data.read())
        return joblib.load(model_bytes)

    new_prep = data_prep(new_data, trained_features)
    rf_model = get_model('rf_model',db)
    xgb_model = get_model('xgb_model',db)
    rf_prediction = rf_model.predict(new_prep)
    xgb_prediction = xgb_model.predict(new_prep)
    avg_prediction = (rf_prediction + xgb_prediction) / 2
    logger.info(f'Prediction: {avg_prediction[0]}')
    client.close()
    return avg_prediction[0]
