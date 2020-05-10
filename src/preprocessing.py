"""Handles download and preprocessing of competition data."""

import sqlite3
from zipfile import ZipFile

import pandas as pd
from kaggle import api


class Preprocessor:
    """Handles download and preprocessing of competition data."""

    def __init__(self, data_path: str):
        """Initialize Preprocessor instance.

        Arguments:
            data_path {str} -- Target path data storage.
        """
        self.data_path = data_path
        self.zipfile = None
        self.file_generator = None

        self.connection = sqlite3.connect(data_path + '/m5.db')

        self.api_download = api.download_cli

    def execute(self):
        """Execute Preprocessing pipeline."""
        self._download_zip()
        self._open_zip()
        self._stream_to_db()

        self.connection.close()

    def _download_zip(self):
        self.api_download(
            competition='m5-forecasting-accuracy', path=self.data_path
            )

    def _open_zip(self):
        self.zipfile = ZipFile(self.data_path + 'm5-forecasting-accuracy.zip')

    def _stream_to_db(self):
        filenames = (
            'calendar.csv',
            'sell_prices.csv',
            'sales_train_validation.csv'
            )

        file_generator = (
            (name, pd.read_csv(self.zipfile.open(name), chunksize=100))
            for name in filenames
            )

        upload = map(
            lambda name, chunk_generator:
            self._upload_file(name, chunk_generator), file_generator
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
                chunk_generator
        )

        _ = tuple(uploader)

    @staticmethod
    def _sales_to_long(self, sub_table):
        result = sub_table.melt(
            id_vars=['id', 'item_id', 'dept_id', 'cat_id_store', 'state_id'],
            var_name='d',
            value_name='sales'
        )

        return result
