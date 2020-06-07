"""Module to test DBGenerator Class."""

import pytest
from db_generator import DBGenerator

WORK_DIR = './data'


@pytest.fixture(scope='session')
def db_gen():
    """Create proper DB from direcory"""
    db_gen = DBGenerator.from_directory('./data')
    db_gen.create_db()


def from_zipfile_succeeds_proper_file():
    """Test if class can be instantiated from zipfile."""
    db_gen = DBGenerator.from_zipfile(WORK_DIR)


def from_zipfile_fails_bad_path():
    """Test if class instantiation fails with bad path."""
    db_gen = DBGenerator.from_zipfile('./bad_data')