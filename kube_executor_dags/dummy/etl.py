from datetime import date

from fs_etl.base_task import BaseEtlTask


class MmbEtlTask(BaseEtlTask):
    def process_data(self, calc_date: date):
        print("==============================")
        print("MmbEtlTask.process_data(...)")
        print("==============================")

        ti = self.airflow_kwargs["ti"]
        ti.xcom_push(key="mmb_data", value={"some": "data", "you": "want"})

        print(f"PySpark version: {self.spark_session.version}")
        print(f"date_interval: {calc_date}")

        self.spark_session.sql("SHOW DATABASES").show()

        print("")

        # print(">> SRC SETTINGS:")
        # print(self.settings.src_settings)
        # print(">> DEST SETTINGS:")
        # print(self.settings.dest_settings)

    # def run_pre_checks(self, calc_date: date):
    #     # super().run_pre_checks(calc_date)
    #     print(">> user pre-checks")

    def save_metadata(self, calc_date: date, success: bool = True):
        # super().save_metadata(calc_date, success)
        print(f">> user save_metadata, calc_date: {calc_date}, success: {success}")

    # def run_post_checks(self, calc_date: date):
    #     # super().run_post_checks(calc_date)
    #     # super().run_metrics(calc_date)
    #     print(">> user post-checks")
