import logging
import os

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, GridSearchCV, KFold
from sklearn.ensemble import RandomForestClassifier
from xgboost import XGBClassifier
import joblib
from pymongo import MongoClient
import gridfs
import io
from db import get_dataset_df


logger = logging.getLogger(__name__)


m_string = os.getenv('m_string')


def data_prep(df):
    logger.info('Preparing data for model training')
    df['desc'] = (df['perfumer'] + ',' + df['brand_name'] + ',' + df['notes'] + ',' + df[
        'accords']).str.lower().str.replace('[()-]', ' ', regex=True).str.split(',')
    perfume_info = df[['perfume_id', 'desc', 'vote']]
    perfume_info = perfume_info.explode('desc')
    perfume_info = pd.get_dummies(perfume_info, columns=['desc'], prefix='')
    perfume_info = perfume_info.groupby(['perfume_id', 'vote'], as_index=False).sum()
    perfume_info = perfume_info.drop(['_'], axis=1)
    return perfume_info


def RandomForest_result(final_df):
    logger.info('RF model training started')
    X = final_df.drop(['perfume_id','vote'], axis=1)
    y = final_df[['vote']]
    y = np.squeeze(y)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    rf_model = RandomForestClassifier(n_estimators=100)
    cv = KFold(n_splits=10, shuffle=True, random_state=42)
    param_grids = {
        'max_depth': [10,20,30,35]
    }
    grids = GridSearchCV(estimator=rf_model, param_grid=param_grids, cv=cv, scoring='accuracy', n_jobs=-1, verbose=2, error_score='raise')
    grids.fit(X_train, y_train)
    best_params = grids.best_params_
    best_score = grids.best_score_
    logger.info(f'Best RandomForest parameters: {best_params}')
    logger.info(f'Best RandomForest score: {best_score}')
    rf_model = RandomForestClassifier(n_estimators=10000, max_depth=best_params['max_depth'])
    rf_model.fit(X_train, y_train)
    logger.info('RF model trained')
    return rf_model


def XGB_result(final_df):
    logger.info('XGB model training started')
    X = final_df.drop(['perfume_id', 'vote'], axis=1)
    y = final_df[['vote']]
    y = np.squeeze(y)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
    xgb_model = XGBClassifier(n_estimators=100)
    cv = KFold(n_splits=10, shuffle=True, random_state=42)
    param_grids = {
        'max_depth': [10, 20, 30, 35]
    }
    grids = GridSearchCV(estimator=xgb_model, param_grid=param_grids, cv=cv, scoring='accuracy', n_jobs=-1, verbose=2,
                         error_score='raise')
    grids.fit(X_train, y_train)
    best_params = grids.best_params_
    best_score = grids.best_score_
    logger.info(f'Best XGB parameters: {best_params}')
    logger.info(f'Best XGB score: {best_score}')
    xgb_model = XGBClassifier(n_estimators=10000, max_depth=best_params['max_depth'])
    xgb_model.fit(X_train, y_train)
    logger.info('XGB model trained')
    return xgb_model


def train_model():
    df = get_dataset_df()
    df_prep = data_prep(df)
    features = df_prep.drop(['perfume_id','vote'], axis=1).columns.tolist()
    rf_model = RandomForest_result(df_prep)
    xgb_model = XGB_result(df_prep)
    client = MongoClient(m_string)
    db = client['scent_db']
    collection = db['training_features']
    list_document = {'list_name': 'scent_train', 'list_data': features}
    collection.update_one(
        {'list_name': 'scent_train'},
        {'$set': list_document},
        upsert=True
    )
    logger.info(f'Training features have been saved')

    def load_model(model_name, client):
        logger.info(f'{model_name} loading')
        model_bytes = io.BytesIO()
        joblib.dump(model_dict[model_name], model_bytes)
        model_bytes.seek(0)
        db = client['scent_db']
        fs = gridfs.GridFS(db)
        existing_file = fs.find_one({'filename': f'{model_name}.pkl'})
        if existing_file:
            fs.delete(existing_file._id)
        fs.put(model_bytes, filename=f'{model_name}.pkl')
        logger.info(f'{model_name} saved')

    model_dict = {'rf_model': rf_model, 'xgb_model': xgb_model}
    for model in model_dict.keys():
        load_model(model, client)
    client.close()
