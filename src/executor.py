"""Executor Module for M5-Forecasting-Accuracy Project."""

import yaml

from dm_generator import DMGenerator


class Executor:
    """Executor class."""

    def __init__(self, config_path=None):
        """Initialize Executor Class."""
        self.folder = None
        self.zipfile_path = None
        self.purge = None

        self._set_config(config_path)

    def _set_config(self, config_path):
        if not config_path:
            config_path = './config/config.yaml'
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        self.folder = config["FOLDER"]
        self.zipfile_path = config["ZIPFILE_PATH"]
        self.purge = config["PURGE"]

    def execute(self):
        """Execute pipeline."""
        self._create_datamart()

    def _create_datamart(self):
        dmg = DMGenerator(
            folder=self.folder,
            zipfile_path=self.zipfile_path,
            purge=self.purge
        )

        dmg.generate_datamart()


if __name__ == '__main__':
    exe = Executor()
    exe.execute()
