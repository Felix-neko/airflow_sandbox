from datetime import date

from fs_etl.base_task import BaseEtlTask


class BigFatEtlTask(BaseEtlTask):
    def process_data(self, logical_date: date):
        print("==============================")
        print("BigFatEtlTask.process_data(...)")
        print("==============================")

        print(f"PySpark version: {self.spark_session.version}")
        print(f"date_interval: {logical_date}")

        self.spark_session.sql("SHOW DATABASES").show()

        print(">> SRC SETTINGS:")
        print(self.settings.src_settings)
        print(">> DEST SETTINGS:")
        print(self.settings.dest_settings)

        print(">> AIRFLOW KWARGS:")
        print(self.airflow_kwargs)

        print(">> PREV DATA FROM MMB")
        print(self.airflow_kwargs["data_from_mmb"])

    def save_metadata(self, scheduled_date: date, logical_date: date, success: bool = True):
        # super().save_metadata(calc_date, success)
        print(f">> user save_metadata, calc_date: {logical_date}, success: {success}")