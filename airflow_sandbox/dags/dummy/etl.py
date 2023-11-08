from datetime import date

import numpy as np
import pandas as pd

from dateutil.relativedelta import relativedelta

from fs_etl.base_task import BaseEtlTask


import some_module
from cassandra_dumper import create_test_arrow_table


class MmbEtlTask(BaseEtlTask):

    def get_logical_date(self, scheduled_date: date) -> date:
        return scheduled_date

    def process_data(self, logical_date: date):

        print(logical_date)
        print(self.airflow_kwargs)


class DummyEtlTask(BaseEtlTask):

    def process_data(self, logical_date: date):
        print(type(self).__name__)
        print(logical_date)
        print(self.airflow_kwargs)