"""Test module for executor."""

import os
import shutil

import pytest

from src.executor import Executor

CONFIG_PATH = './test/data/test_config.yaml'
TEST_FOLDER = './test/test_folder'


@pytest.fixture
def exe():
    exe = Executor(config_path=CONFIG_PATH)

    return exe


def execute_creates_datamart_test(exe):
    os.mkdir(TEST_FOLDER)

    exe.execute()
    content = os.listdir(TEST_FOLDER)
    assert len(content) > 0

    shutil.rmtree(TEST_FOLDER)
