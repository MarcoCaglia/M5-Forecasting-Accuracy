"""Module for DB creation for the M5-Project."""

import logging
import os
import sqlite3
import types
from zipfile import ZipFile

import pandas as pd
from kaggle import api
from tqdm import tqdm

COMP_NAME = 'm5-forecasting-accuracy'


class DBGenerator:
    """Class to generate the DB for the M5-Project."""

    def __init__(self, generator_dict, work_dir):
        """Initialize DBGenerator."""
        self.generator = generator_dict

        self.logger = logging.getLogger()

        self.logger.info('Initialized')

        self._validate_generator()

        self.connection = sqlite3.connect(work_dir + 'm5.db')

    def create_db(self):
        """Create Database."""
        self._create_sales_info_table()
        self._create_other_table('calendar')
        self._create_other_table('price')

    def _create_sales_info_table(self):
        self.logger.info('Creating info and sales table...')

        for chunk in tqdm(self.generator['sales']):
            sales_table, info_table = self._split_table(chunk)

            info_table.to_sql(
                'info', if_exists='append', index=False, con=self.connection
                )

            sales_table.to_sql(
                'sales', if_exists='append', index=False, con=self.connection
            )

        self.logger.info('Sales and info tables created.')

    def _create_other_table(self, table_name):
        self.logger.info(f'Creating {table_name} table...')
        for chunk in self.generator[table_name]:
            chunk.to_sql(
                table_name,
                con=self.connection,
                index=False,
                if_exists='append'
                )

        self.logger.info(f'{table_name} table created...')

    def _validate_generator(self):
        self.logger.info('Validating generator dict...')
        expected_keys = {'sales', 'calendar', 'price'}

        assert expected_keys == set(self.generator)

        for key in self.generator:
            assert isinstance(self.generator[key], types.GeneratorType)

        self.logger.info('Validation Successful!')

    @staticmethod
    def _split_and_melt(chunk):
        info_cols = {
            'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id'
        }

        sales_cols = set(chunk) - info_cols

        sales = chunk.loc[:, sales_cols].copy()
        info = chunk.loc[:, {'id'}.union(info_cols)].copy()

        sales = sales.melt(
            id_vars=['id'],
            var_name='day',
            value_name='sales'
        )

        return sales, info

    @staticmethod
    def _get_logger():
        logging.basicConfig(
            filename='db_generator.log',
            format="%(levelname)s %(asctime)s - %(message)s",
            level=20,
            filemode='w'
        )

        return logging.getLogger()

    @classmethod
    def from_api(cls, work_dir, persist=False):
        """Construct DB Generator with Kaggle API."""
        api.competition_download_cli(
            COMP_NAME,
            work_dir
        )
        zip_path = work_dir + '/' + COMP_NAME + '.zip'
        zipfile = ZipFile(zip_path)

        if not persist:
            os.remove(zip_path)

        generator_dict = {
            'sales': pd.read_csv(
                zipfile.open('sales_train_validation.csv'), chunksize=10000
                ),
            'calendar': pd.read_csv(
                zipfile.open('calendar.csv'), chunksize=10000
            ),
            'price': pd.read_csv(
                zipfile.open('sell_prices.csv'), chunksize=10000
            )
        }

        db_gen = cls(generator_dict, work_dir)

        return db_gen

    @classmethod
    def from_zipfile(cls, work_dir):
        """Construct DB generator from Zipfile."""
        zip_path = work_dir + '/' + COMP_NAME + '.zip'
        zipfile = ZipFile(zip_path)

        generator_dict = {
            'sales': pd.read_csv(
                zipfile.open('sales_train_validation.csv'), chunksize=10000
                ),
            'calendar': pd.read_csv(
                zipfile.open('calendar.csv'), chunksize=10000
            ),
            'price': pd.read_csv(
                zipfile.open('sell_prices.csv'), chunksize=10000
            )
        }

        db_gen = cls(generator_dict, work_dir)

        return db_gen

    @classmethod
    def from_directory(cls, work_dir):
        """Generate DB Generator from directory with csv files."""
        generator_dict = {
            'sales': pd.read_csv(
                f'{work_dir}/sales_train_validation.csv', chunksize=10000
                ),
            'calendar': pd.read_csv(
                f'{work_dir}/calendar.csv', chunksize=10000
                ),
            'price': pd.read_csv(
                f'{work_dir}/sell_prices.csv', chunksize=10000
                )
        }

        db_gen = cls(generator_dict, work_dir)

        return db_gen
