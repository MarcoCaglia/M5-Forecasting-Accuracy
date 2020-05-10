"""Preprocessor Test module."""

import unittest
from preprocessing import Preprocessor
import os


class TestPreprocessor(unittest.TestCase):
    """Testmodule for Preprocessing."""

    def test_preprocessing(self):
        """Test full preprocessing pipeline."""
        data_path = '../data'
        prepper = Preprocessor(data_path)

        prepper.execute()

        files = os.listdir(data_path + '/')

        assert 'm5.db' in files
