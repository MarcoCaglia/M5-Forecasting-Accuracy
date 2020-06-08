"""Module for time series EDA."""
import logging

from dask.distributed import Client
from dask_ml.linear_model import LinearRegression


class TSTools:
    """Tool for general time series EDA."""

    def __init__(self, time_series, workers=4, memory_limit='8GB'):
        """Initialize TSTools."""
        self.logger = self.get_logger()

        self.logger.info('Initializing dask client...')
        self.client = Client(n_workers=workers, memory_limit=memory_limit)
        self.logger.info(
            f'Initialization sucessful!'
            f'Dashboard: {self.client.dashboard_link}'
            )

        self.time_series = time_series
        self.seasonal_effects = None
        self.seasonal_r2 = None
        self.trend = None
        self.auto_correlation = None
        self.external_correlation = None

    def add_seasonal_effects(
        self,
        exp_vars: list,
        dep_vars: str,
        add_interaction=False
    ):
        """Extract seasonal effects from time series.

        Args:
            exp_vars (list): Explanator variables.
            dep_vars (str): Dependat variables.
            add_interaction (bool, optional): Usage of interaction terms
                                              between explanatory variables.
                                              Defaults to False.
        """
        self.logger.info('Scheduling Seasonal Effect extraction...')
        X = self.time_series.loc[:, exp_vars]
        y = self.time_series.loc[:, dep_vars]

        model = LinearRegression()

        model.fit(X, y)
        self.seasonal_r2 = model.score(X, y)
        self.seasonal_effects = {
            key: coef
            for key, coef
            in zip(exp_vars, model.coef_)
            }

    def add_trend_effect(self):
        """Extract the time trend of a time series."""
        pass

    def add_auto_correlation(self):
        """Analyze autocorrelation of Time Series."""
        pass

    def add_correlations(self):
        """Analyze external correlation of Time Series."""
        pass

    def get_results(self, dry=True):
        """Compute and return scheduled results."""
        if dry:
            result = (self.seasonal_r2, self.seasonal_effects)
        else:
            result = (
                self.seasonal_r2.compute(),
                self.seasonal_effects.compute()
                )

        return result

    @staticmethod
    def _get_logger():
        logging.basicConfig(
            filename='TSTools.log',
            format="%(levelname)s %(asctime)s - %(message)s",
            level=20,
            filemode='w'
        )

        logger = logging.getLogger('TSTools')

        return logger
