import logging
import os

import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)


c_string = os.getenv('c_string')
ca_string = os.getenv('ca_string')

engine = create_engine(ca_string)


def insert_data(table_name, insert_list):
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
    query = '''
        SELECT p.*, b.brand_name, CONCAT(p.perfume_name, ', ', b.brand_name) AS full_name
        FROM perfumes_catalog p
        LEFT JOIN brands b ON p.brand_id = b.brand_id
        ORDER BY b.brand_name, p.perfume_name;
    '''
    df = pd.read_sql_query(query, con=engine)
    return df


def get_dataset_df():
    query = '''
        SELECT c.perfume_name, p.*, b.brand_name, v.vote 
        FROM perfumes_data p
        INNER JOIN my_votes v ON p.perfume_id = v.perfume_id 
        INNER JOIN perfumes_catalog c ON p.perfume_id = c.perfume_id 
        INNER JOIN brands b ON b.brand_id = c.brand_id
    '''
    df = pd.read_sql_query(query, con=engine)
    return df

def get_pred_df(perfume_name):
    query = text('''
        SELECT c.perfume_name, p.*, b.brand_name
        FROM perfumes_data p 
        INNER JOIN perfumes_catalog c ON p.perfume_id = c.perfume_id 
        INNER JOIN brands b ON b.brand_id = c.brand_id 
        WHERE CONCAT(c.perfume_name, ', ', b.brand_name) = :perfume_name;
    ''')
    df = pd.read_sql_query(query, con=engine, params={"perfume_name": perfume_name})
    return df


def get_perfume_url(full_name):
    query = text('''
        SELECT c.perfume_id, c.perfume_url 
        FROM perfumes_catalog c 
        INNER JOIN brands b ON c.brand_id = b.brand_id 
        WHERE CONCAT(c.perfume_name, ', ', b.brand_name) = :full_name;
    ''')
    result = pd.read_sql_query(query, con=engine, params={"full_name": full_name}).values.tolist()
    if result:
        return result[0]
    else:
        raise ValueError(f'No results found for {full_name}')


def get_votes_full_data():
    query = '''
        SELECT p.*, v.vote, CONCAT(p.perfume_name, ', ', b.brand_name) AS full_name
        FROM perfumes_catalog p 
        INNER JOIN my_votes v ON p.perfume_id = v.perfume_id 
        INNER JOIN brands b ON p.brand_id = b.brand_id
    '''
    df = pd.read_sql_query(query, con=engine)
    logger.info(f'Votes full data is ready')
    return df


def get_table_df(table_name):
    return pd.read_sql_table(table_name, con=engine)


