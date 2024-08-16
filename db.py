import logging
import os

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


tables_dict = {'perfumes_catalog': 'perfume_id, perfume_nickname, perfume_name, perfume_url, brand_id',
               'my_votes': 'perfume_id, vote',
               'dataset': 'review_id, perfume_id, perfume_name, brand_name, perfumer, accords, notes, reviewer, reviewer_id, review, vote',
               'brands': 'brand_id, brand_name, brand_url',
               'perfumes_data': 'perfume_id,perfumer, accords, notes, rating, votes_number',
               'reviews_data': 'review_id, perfume_id, reviewer_id, review, review_tone',
               'reviewers': 'reviewer_id, reviewer'}


c_string = os.getenv('c_string')
ca_string = os.getenv('ca_string')


def insert_data(table_name, insert_list):
    engine = create_engine(ca_string)
    logger.info(f'New data inserting into {table_name} started')

    tables_fixed = {'perfumes_catalog': 'perfume_id', 'perfumes_data': 'perfume_id', 'brands': 'brand_id',
                    'reviews_data': 'review_id', 'reviewers': 'reviewer_id'}
    tables_flex = {'my_votes': 'perfume_id'}

    metadata = MetaData()
    metadata.reflect(bind=engine)

    if table_name not in metadata.tables:
        raise ValueError(f'Table {table_name} does not exist in the database')

    table = Table(table_name, metadata, autoload_with=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        if table_name in tables_fixed:
            stmt = insert(table).values(insert_list)
            stmt = stmt.on_conflict_do_nothing(index_elements=[tables_fixed[table_name]])
        elif table_name in tables_flex:
            stmt = insert(table).values(insert_list)
            stmt = stmt.on_conflict_do_update(
                index_elements=[tables_flex[table_name]],
                set_={'vote': stmt.excluded.vote}
            )
        else:
            raise ValueError(f'Unknown table type for {table_name}')
        session.execute(stmt)
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f'Error inserting data into {table_name}: {e}')
        raise
    finally:
        session.close()


def get_full_data():
    engine = create_engine(ca_string)
    query = f'''
        SELECT p.*, b.brand_name,p.perfume_name||', '||b.brand_name as full_name  FROM perfumes_catalog p
        LEFT JOIN brands b ON p.brand_id=b.brand_id order by brand_name,perfume_name;
        '''
    df = pd.read_sql_query(query,con=engine)
    return df


def get_dataset_df():
    engine = create_engine(ca_string)
    query = f'''SELECT c.perfume_name,p.*,b.brand_name,v.vote 
                FROM perfumes_data p inner join my_votes v on p.perfume_id = v.perfume_id 
                inner join perfumes_catalog c on p.perfume_id = c.perfume_id 
                inner join brands b on b.brand_id = c.brand_id'''
    df = pd.read_sql_query(query, con=engine)
    return df


def get_pred_df(perfume_name):
    engine = create_engine(ca_string)
    query = text('''SELECT c.perfume_name,p.*,b.brand_name
                FROM perfumes_data p 
                inner join perfumes_catalog c on p.perfume_id = c.perfume_id 
                inner join brands b on b.brand_id = c.brand_id 
                where c.perfume_name||', '||b.brand_name = :perfume_name;
                ''')
    df = pd.read_sql_query(query, con=engine, params={"perfume_name": perfume_name})
    return df


def get_perfume_url(full_name):
    engine = create_engine(ca_string)
    query = text('''SELECT perfume_id,perfume_url 
                FROM perfumes_catalog c 
                INNER JOIN brands b ON c.brand_id = b.brand_id 
                WHERE c.perfume_name||', '||b.brand_name = :full_name;
                ''')
    perfume_id,url = pd.read_sql_query(query, con=engine, params={"full_name": full_name}).values.tolist()[0]
    return perfume_id,url


def get_votes_full_data():
    engine = create_engine(ca_string)
    query = f'''SELECT p.*,v.vote,p.perfume_name||', '||b.brand_name as full_name 
                FROM perfumes_catalog p 
                INNER JOIN votes v on p.perfume_id = v.perfume_id 
                INNER JOIN brands b ON p.brand_id = b.brand_id;
                '''
    df = pd.read_sql_query(query,con=engine)
    logger.info(f'Votes full data is ready')
    return df


def get_table_df(table_name):
    engine = create_engine(ca_string)
    return pd.read_sql_table(table_name, con=engine)


