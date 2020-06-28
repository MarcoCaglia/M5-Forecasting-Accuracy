"""Test module for data mart generator."""

import json
import os
import shutil

import pytest

from src.dm_generator import (DMGenerator, InvalidDirectoryError,
                              PopulatedDirectoryError)

TEST_FOLDER = './test/test_folder'
TEST_ZIP = './test/data/dm_test_data.zip'
FOLDERS = ['sales', 'info', 'calendar', 'prices']


@pytest.fixture
def dmg():
    dmg = DMGenerator(
        folder=TEST_FOLDER,
        zipfile_path=TEST_ZIP,
        )

    return dmg


def generate_datamart_nonexistant_folder_failure_test(dmg):
    with pytest.raises(InvalidDirectoryError):
        dmg.generate_datamart()


def generate_datamart_existant_folder_success_test(dmg):
    os.mkdir(TEST_FOLDER)
    dmg.generate_datamart()

    for folder in FOLDERS:
        test_path = f'{TEST_FOLDER}/{folder}'
        assert os.path.isdir(test_path)
        assert len(os.listdir(test_path)) > 0

    shutil.rmtree(TEST_FOLDER)


def generate_datamart_no_purge_override_fails_test(dmg):
    os.mkdir(TEST_FOLDER)
    with open(TEST_FOLDER + '/fake_file.json', 'w') as f:
        json.dump({}, f)
    with pytest.raises(PopulatedDirectoryError):
        dmg.generate_datamart()

    shutil.rmtree(TEST_FOLDER)


def generate_datamart_purge_override_success_test():
    os.mkdir(TEST_FOLDER)
    with open(TEST_FOLDER + '/fake_file.json', 'w') as f:
        json.dump({}, f)

    dmg = DMGenerator(
        folder=TEST_FOLDER,
        zipfile_path=TEST_ZIP,
        purge=True
    )
    dmg.generate_datamart()
    for folder in FOLDERS:
        test_path = f'{TEST_FOLDER}/{folder}'
        assert os.path.isdir(test_path)
        assert len(os.listdir(test_path)) > 0

    shutil.rmtree(TEST_FOLDER)
