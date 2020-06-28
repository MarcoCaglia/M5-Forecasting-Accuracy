"""Module for Data Mart generation."""

import os
import shutil
from zipfile import ZipFile

import pandas as pd


class InvalidDirectoryError(Exception):
    """Passed path was not a folder."""

    pass


class PopulatedDirectoryError(Exception):
    """Selected Directory is already populated."""

    pass


class DMGenerator:
    """Class for DataMart generation."""

    SALES_FOLDER = 'sales'
    INFO_FOLDER = 'info'
    CALENDAR_FOLDER = 'calendar'
    PRICES_FOLDER = 'prices'

    def __init__(self, folder, zipfile_path, purge=False):
        """Initialize DMGenerator."""
        self.folder_path = os.path.abspath(folder)
        self.sales, self.price, self.calendar = self._get_generators(
            zipfile_path
            )
        self.purge = purge

    def generate_datamart(self):
        """Execute data mart generation."""
        self._validate_directory()
        self._make_directories()
        self._write_files()

    def _write_files(self):
        self._write_sales_info()
        self._write_prices()
        self._write_calendar()

    def _write_sales_info(self):
        info_data = pd.DataFrame()
        for index, chunk in enumerate(self.sales):
            info_cols = [
                'item_id',
                'dept_id',
                'cat_id',
                'store_id',
                'state_id'
                ]
            info_data = info_data.append(chunk.loc[:, ['id'] + info_cols])
            sales_cols = [col for col in chunk.columns if 'd_' in col]
            sales = chunk.loc[:, ['id'] + sales_cols].melt(id_vars='id')
            sales.to_hdf(
                self.folder_path + '/' + DMGenerator.SALES_FOLDER
                + f'/sales.h5',
                key=f'index_{index}',
                mode='a',
                complevel=9
                )
        info_data.drop_duplicates().to_parquet(
            self.folder_path + '/' + DMGenerator.INFO_FOLDER +
            '/info.parquet.gzip', index=False
        )

    def _write_prices(self):
        for index, chunk in enumerate(self.price):
            chunk.to_hdf(
                self.folder_path + '/' + DMGenerator.PRICES_FOLDER +
                '/prices.h5',
                key=f'index_{index}',
                mode='a',
                complevel=9
            )

    def _write_calendar(self):
        for index, chunk in enumerate(self.calendar):
            chunk.to_hdf(
                self.folder_path + '/' + DMGenerator.CALENDAR_FOLDER +
                '/calendar.h5',
                key=f'index_{index}',
                mode='a',
                complevel=9
            )

    def _make_directories(self):
        current_content = os.listdir(self.folder_path)
        if self.purge:
            for content in current_content:
                if os.path.isdir(self.folder_path + '/' + content):
                    shutil.rmtree(self.folder_path + '/' + content)
                else:
                    os.remove(self.folder_path + '/' + content)
        elif len(current_content) > 0:
            raise PopulatedDirectoryError(
                "Selected Directory is already populated."
                )

        folders = [
            DMGenerator.SALES_FOLDER,
            DMGenerator.PRICES_FOLDER,
            DMGenerator.CALENDAR_FOLDER,
            DMGenerator.INFO_FOLDER
            ]
        for folder in folders:
            os.mkdir(self.folder_path + '/' + folder)

    def _validate_directory(self):
        if not os.path.isdir(self.folder_path):
            raise InvalidDirectoryError(
                "Please make sure the directory exists before trying to"
                "establish a data mart there."
                )

    @staticmethod
    def _get_generators(zipfile_path):
        zipfile = ZipFile(zipfile_path)

        sales = pd.read_csv(
            zipfile.open("sales_train_validation.csv"),
            chunksize=1000
            )
        price = pd.read_csv(
            zipfile.open("sell_prices.csv"),
            chunksize=100000
            )
        calendar = pd.read_csv(
            zipfile.open("calendar.csv"),
            chunksize=100000
            )

        return sales, price, calendar
