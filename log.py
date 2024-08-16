import logging
import os

from pymongo import MongoClient, errors


m_string = os.getenv('m_string')

class MongoDBHandler(logging.Handler):
    def __init__(self, mongo_uri, db_name, collection_name):
        super().__init__()
        self.client = None
        self.db = None
        self.collection = None
        try:
            self.client = MongoClient(mongo_uri, serverSelectionTimeoutMS=1000)  # 5 seconds timeout
            self.client.server_info()
            self.db = self.client[db_name]
            self.collection = self.db[collection_name]
        except errors.ServerSelectionTimeoutError as err:
            logging.error(f"Failed to connect to MongoDB: {err}")
            if self.client is not None:
                self.client.close()
            self.client = None
            self.db = None
            self.collection = None

    def emit(self, record):
        if self.collection is not None:
            log_entry = self.format(record)
            self.collection.insert_one({'log': log_entry})

    def close(self):
        if self.client is not None:
            self.client.close()
        super().close()


def setup_logging():
    mongo_uri = m_string
    db_name = 'scent_db'
    collection_name = 'scent_recommender_logs'
    logger = logging.getLogger()
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)

        mongo_handler = MongoDBHandler(mongo_uri, db_name, collection_name)
        mongo_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(mongo_handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(console_handler)

    logger.info("Logging setup complete")

