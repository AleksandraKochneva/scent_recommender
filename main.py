import logging
from db import insert_data
from parsers import get_brands_by_perfume, get_brand_catalog, perfume_data_parser


logger = logging.getLogger(__name__)


def update_data(new_perfumes):
    logger.info('Updating perfumes_data, reviews_data, reviewers')
    temp = []
    for row in new_perfumes:
        try:
            perfume_id, perfume_url = row
        except:
            perfume_id = row[0]
            perfume_url = row[-2]
        data = perfume_data_parser(perfume_url)
        if data != 'fail':
            perfumer, accords, notes, rating, votes_number, reviews_data = data
            insert_list = [[perfume_id,perfumer,accords,notes,rating,votes_number]]
            insert_data('perfumes_data', insert_list)
            logger.info(f'{perfume_url} added to perfumes_data')
            if reviews_data:
                insert_list = [(f'{perfume_id}_{el[0]}',perfume_id,el[0],el[2],el[3]) for el in reviews_data]
                insert_data('reviews_data', insert_list)
                logger.info(f'{perfume_url} added to reviews_data')
                insert_list = [(el[0],el[1]) for el in reviews_data]
                insert_data('reviewers', insert_list)
                logger.info(f'{perfume_url} added to reviewers')
            else:
                logger.info(f'No reviews in {perfume_url}')
        else:
            logger.error(f'Can not parse "{row[1]},{row[2]}" reviews')
            break
    if temp and temp != 'fail':
        logger.info('Dataset has been updated')


def query_catalog_parser(perfume_name):
    logger.info('Adding new data to catalog')
    new_brands_data = get_brands_by_perfume(perfume_name)
    if new_brands_data:
        flag = True
        for brand_data in new_brands_data:
            insert_list = get_brand_catalog(brand_data)
            if insert_list != 'fail':
                insert_data('perfumes_catalog', insert_list)
                logger.info(f'{brand_data[0][-1]} has been added to catalog')
            else:
                flag = False
                break
        return flag
    else:
        return 'empty'
