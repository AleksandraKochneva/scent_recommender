import logging
import os

import pandas as pd
from sqlalchemy import text, create_engine, MetaData, Table, Column, Integer, String, Float, Boolean, ForeignKey, Index, insert
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)


c_string = os.getenv('c_string')
ca_string = os.getenv('ca_string')

engine = create_engine(ca_string)


def insert_data(table_name, insert_list, engine = engine):
    logger.info(f'New data inserting into {table_name} started')

    metadata = MetaData()

    table_schemas = {
        'brands': Table(
            'brands', metadata,
            Column('brand_id', String, primary_key=True),
            Column('brand_name', String),
            Column('brand_url', String)
        ),
        'my_votes': Table(
            'my_votes', metadata,
            Column('perfume_id', Integer, primary_key=True),
            Column('vote', Boolean)
        ),
        'perfumes_catalog': Table(
            'perfumes_catalog', metadata,
            Column('perfume_id', Integer, primary_key=True),
            Column('perfume_nickname', String),
            Column('perfume_name', String),
            Column('perfume_url', String),
            Column('brand_id', String),
            Index('idx_brand_id', 'brand_id'),
            Index('idx_perfume_name', 'perfume_name'),
            Index('idx_perfume_id_brand_id', 'perfume_id', 'brand_id')
        ),
        'perfumes_data': Table(
            'perfumes_data', metadata,
            Column('perfume_id', Integer, primary_key=True),
            Column('perfumer', String),
            Column('accords', String),
            Column('notes', String),
            Column('rating', Float),
            Column('votes_number', Integer)
        ),
        'reviewers': Table(
            'reviewers', metadata,
            Column('reviewer_id', String, primary_key=True),
            Column('reviewer', String)
        ),
        'reviews_data': Table(
            'reviews_data', metadata,
            Column('review_id', String, primary_key=True),
            Column('perfume_id', Integer, ForeignKey('perfumes_catalog.perfume_id')),
            Column('reviewer_id', String, ForeignKey('reviewers.reviewer_id')),
            Column('review', String),
            Column('review_tone', Boolean)
        )
    }

    metadata.reflect(bind=engine)

    if table_name not in metadata.tables:
        logger.info(f'Table {table_name} does not exist. Creating it.')
        if table_name in table_schemas:
            table_schemas[table_name].create(engine)
            metadata.create_all(engine)
        else:
            raise ValueError(f'Unknown table schema for {table_name}')

    table = Table(table_name, metadata, autoload_with=engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        if table_name in ['perfumes_catalog', 'brands', 'perfumes_data', 'reviewers', 'reviews_data']:
            stmt = insert(table).values(insert_list)
            stmt = stmt.on_conflict_do_nothing(
                index_elements=[table.columns[table_schemas[table_name].primary_key.columns.keys()[0]]])
        elif table_name == 'my_votes':
            stmt = insert(table).values(insert_list)
            stmt = stmt.on_conflict_do_update(
                index_elements=['perfume_id'],
                set_={'vote': stmt.excluded.vote}
            )
        else:
            raise ValueError(f'Unknown table type for {table_name}')

        session.execute(stmt)
        session.commit()
    except SQLAlchemyError as e:
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


