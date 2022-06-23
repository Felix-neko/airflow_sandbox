from datetime import date

from fs_etl.base_task import BaseEtlTask


class BigFatEtlTask(BaseEtlTask):
    def process_data(self, calc_date: date):
        print("==============================")
        print("BigFatEtlTask.process_data(...)")
        print("==============================")

        print(f"PySpark version: {self.spark_session.version}")
        print(f"date_interval: {calc_date}")

        self.spark_session.sql("SHOW DATABASES").show()

        print(">> SRC SETTINGS:")
        print(self.settings.src_settings)
        print(">> DEST SETTINGS:")
        print(self.settings.dest_settings)

        print(">> AIRFLOW KWARGS:")
        print(self.airflow_kwargs)

        print(">> PREV DATA FROM MMB")
        print(self.airflow_kwargs["data_from_mmb"])

    def save_metadata(self, calc_date: date, success: bool = True):
        # super().save_metadata(calc_date)
        print(">> user save_metadata")
