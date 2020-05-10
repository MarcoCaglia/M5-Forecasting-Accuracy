"""Handles download and preprocessing of competition data."""

import logging
import sqlite3
from zipfile import ZipFile
from tqdm import tqdm

import pandas as pd
from kaggle import api


class Preprocessor:
    """Handles download and preprocessing of competition data."""

    def __init__(self, data_path: str):
        """Initialize Preprocessor instance.

        Arguments:
            data_path {str} -- Target path data storage.
        """
        LOG_FORMAT = "%(levelname)s %(asctime)s - %(message)s"
        logging.basicConfig(
            filename='preprocessing_log.log',
            level=20,
            format=LOG_FORMAT,
            filemode='w'
        )

        logging.info('Initializing...')

        self.data_path = data_path
        self.zipfile = None
        self.file_generator = None

        self.connection = sqlite3.connect(data_path + '/m5.db')

        self.api_download = api.competition_download_cli

    def execute(self):
        """Execute Preprocessing pipeline."""
        self._download_zip()
        self._open_zip()
        self._stream_to_db()

        self.connection.close()

    def _download_zip(self):
        logging.info('Starting download...')
        self.api_download(
            competition='m5-forecasting-accuracy', path=self.data_path
        )
        logging.info('Data obtained!')

    def _open_zip(self):
        logging.info('Accessing...')
        self.zipfile = ZipFile(self.data_path + '/m5-forecasting-accuracy.zip')
        logging.info('Successfully accessed!')

    def _stream_to_db(self):
        filenames = (
            'calendar.csv',
            'sell_prices.csv',
            'sales_train_validation.csv'
        )

        file_generator = (
            (name, pd.read_csv(self.zipfile.open(name), chunksize=10000))
            for name in filenames
        )

        logging.info('Starting chunkwise upload...')
        upload = map(
            lambda pair:
            self._upload_file(pair[0], pair[1]), file_generator
        )

        _ = tuple(upload)

    def _upload_file(self, name, chunk_generator):
        sql_config = {
            'if_exists': 'append',
            'index': False,
            'con': self.connection
        }

        name = name.split('.')[0]
        uploader = map(
            lambda chunk:
            chunk.to_sql(name, **sql_config)
            if name != 'sales_train_validation.csv'
            else self._sales_to_long(chunk).to_sql(name, **sql_config),
                tqdm(chunk_generator)
        )
        logging.info(f'Starting upload for {name}...')
        _ = tuple(uploader)
        logging.info(f'Succesfully uploaded {name}!')

    @staticmethod
    def _sales_to_long(self, sub_table):
        result = sub_table.melt(
            id_vars=['id', 'item_id', 'dept_id', 'cat_id_store', 'state_id'],
            var_name='d',
            value_name='sales'
        )

        return result
