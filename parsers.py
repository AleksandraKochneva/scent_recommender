import logging
import itertools
import os

import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
from fake_useragent import UserAgent
import json
from pymongo import MongoClient
import cloudscraper


logger = logging.getLogger(__name__)


proxy_url = os.getenv('proxy_url')
m_string = os.getenv('m_string')
main_url = os.getenv('main_url')


def get_proxies(url=proxy_url):
    logger.info('get_proxies has started')
    session = requests.Session()
    user_agent = UserAgent()
    random_user_agent = user_agent.random
    headers = {'User-Agent': random_user_agent}

    try:
        with session.get(url, headers=headers, timeout=5) as response:
            if response.status_code == 200:
                proxies = [el.strip() for el in response.text.split('\n') if el.startswith('http')]
                logger.info(f'Found {len(proxies)} proxies')
                return proxies
            else:
                logger.error(f'Proxies url returned status code: {response.status_code}')
    except requests.exceptions.RequestException as e:
        logger.error(f'Failed while parsing proxies: {e}')
    finally:
        session.close()


def get_page_elements(soup):
    logger.info('Getting page elements')
    if soup.find_all('span', string='Perfumer') or soup.find_all('span', string='Perfumers'):
        if soup.find_all('span', string='Perfumer'):
            perfumers_el_parent = soup.find('span', string='Perfumer').find_parent()
        else:
            perfumers_el_parent = soup.find('span', string='Perfumers').find_parent()
        perfumers_box_el = perfumers_el_parent.find_next_sibling()
        perfumer = [el.text for el in perfumers_box_el.find_all('a', href=lambda href: href and "/noses/" in href)]
        if len(perfumer) > 1:
            perfumer = ','.join(perfumer)
        else:
            perfumer = perfumer[0]
    else:
        perfumer = 'unknown'
    accords = ','.join([el.text for el in soup.find_all('div', class_='cell accord-box')])
    notes = ','.join([el.parent.text for el in soup.find_all(
        'a', href=lambda href: href and f"{main_url}/notes" in href)])
    rating_box = soup.find('p', class_='info-note').find_all('span')
    rating = float(rating_box[0].text)
    votes_number = int(rating_box[2].text.replace(',',''))
    review_boxes = soup.find_all('div', itemprop="review")
    reviews_info = []
    for el in review_boxes:
        member_box = el.find('b', class_="idLinkify")
        review_box = el.find('div', itemprop="reviewBody")
        reviewer_id, reviewer, review = (member_box['title'].split('/')[-1], member_box.text, review_box.text.replace('\n\n', ' '))
        reviews_info.append((reviewer_id, reviewer, review, None))
    perfume_page_data = [perfumer,accords,notes,rating,votes_number,reviews_info]
    return perfume_page_data


def perfume_data_parser(perfume_url):
    url = f'{main_url}{perfume_url}'
    scraper = cloudscraper.create_scraper()
    try:
        response = scraper.get(url)
        if response.status_code == 200:
            soup = bs(response.text, 'html.parser')
            data = get_page_elements(soup)
            return data
        else:
            logger.info(f'{url} parsing failed.{response}. Getting proxies')
            proxies_list = get_proxies()
            proxies_cycle = itertools.cycle(proxies_list)
            for i in range(len(proxies_list)):
                proxy = next(proxies_cycle)
                logger.info(f'Checking {proxy}')
                proxies = {'http': proxy, 'https': proxy}
                try:
                    user_agent = UserAgent()
                    random_user_agent = user_agent.random
                    headers = {'User-Agent': random_user_agent}
                    response = scraper.get(url, headers=headers, proxies=proxies,timeout=3)
                    if response.status_code == 200:
                        soup = bs(response.content, 'html.parser')
                        data = get_page_elements(soup)
                        return data
                    else:
                        logger.info(f'{perfume_url} parsing with {proxy} failed, response code: {response.status_code}')
                        continue
                except:
                    continue
    except:
        logger.error('Parsing failed with all options')
        return 'fail'


def get_brands_by_perfume(perfume):
    logger.info('get_brand_url started')
    headers = {
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-platform': '"macOS"',
        'sec-ch-ua-mobile': '?0',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'content-type': 'application/x-www-form-urlencoded',
    }

    data = '{"requests":[{"indexName":"fragrantica_perfumes","params":"attributesToRetrieve=%5B%22naslov%22%2C%22dizajner%22%2C%22godina%22%2C%22url.EN%22%2C%22thumbnail%22%5D&facets=%5B%22designer_meta.category%22%2C%22designer_meta.country%22%2C%22designer_meta.main_activity%22%2C%22designer_meta.parent_company%22%2C%22dizajner%22%2C%22godina%22%2C%22ingredients.EN%22%2C%22nosevi%22%2C%22osobine.EN%22%2C%22rating_rounded%22%2C%22spol%22%5D&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=80&maxValuesPerFacet=10&page=0&query=' + perfume + '&tagFilters="}]}'
    client = MongoClient(m_string)
    db = client['scent_db']
    collection = db['keys']
    query = {"fraga_key": {"$exists": True}}
    document = collection.find_one(query)
    if document:
        url = document.get("fraga_key", "Key not found")
        logger.info(f"Fraga key: {url}")
        client.close()
        response = requests.post(
                url,
                headers=headers,
                data=data,
            )
        if response.status_code == 200:
            data = json.loads(response.text)
            options = data['results'][0]['hits']
            brands_data = [[el['dizajner'],el['url']['EN'][0].split('/')[4], f"/designers/{el['url']['EN'][0].split('/')[4]}.html"] for el in options]
            df = pd.DataFrame({'lists': brands_data})
            df_unique = df.drop_duplicates(subset='lists')
            brands_data = df_unique['lists'].tolist()
            logger.info('Brands data parsed')
            return brands_data[:3]
        else:
            logger.error(f'Could not parse brands data. Response code: {response.status_code}')
            return []
    else:
        logger.error('Key not found')


def get_brand_catalog(brand_data):
    logger.info(f'{brand_data[0]} catalog parsing started')
    url = f'{main_url}{brand_data[2]}'
    scraper = cloudscraper.create_scraper()
    user_agent = UserAgent()
    random_user_agent = user_agent.random
    headers = {'User-Agent': random_user_agent}
    response = scraper.get(url, headers=headers, timeout=5)
    if response.status_code == 200:
        soup = bs(response.text, 'html.parser')
        brand_id = brand_data[2].split('/')[2].replace('.html','')
        all_a = soup.find_all('a', href=lambda href: href and f"/perfume/{url.split('/')[-1].replace('.html','')}" in href)
        perfumes = [(el['href'].split('-')[-1].split('.')[0],'-'.join(el['href'].split('/')[-1].split('-')[:-1]),el.text.strip(),el['href'], brand_id) for el in all_a]
        logger.info(f'{brand_data[0]} library successfully parsed')
        return perfumes
    else:
        logger.error(f'Library parser response code: {response.status_code}')
        return 'fail'