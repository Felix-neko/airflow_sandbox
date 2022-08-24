from datetime import date

from fs_etl.base_task import BaseEtlTask

import some_module


class MmbEtlTask(BaseEtlTask):
    def process_data(self, logical_date: date):

        print("=============================")
        print("checking in-repo imports...")
        print("=============================")
        some_module.foo()

        print("==============================")
        print("MmbEtlTask.process_data(...)")
        print("==============================")

        print(f"PySpark version: {self.spark_session.version}")
        print(f"date_interval: {logical_date}")


        import time
        time.sleep(60)

        # print(">> SRC SETTINGS:")
        # print(self.settings.src_settings)
        # print(">> DEST SETTINGS:")
        # print(self.settings.dest_settings)

    # def run_pre_checks(self, calc_date: date):
    #     # super().run_pre_checks(calc_date)
    #     print(">> user pre-checks")

    # def save_metadata(self, scheduled_date: date, logical_date: date, success: bool = True):
        # super().save_metadata(calc_date, success)
        # print(f">> user save_metadata, calc_date: {logical_date}, success: {success}")

    # def run_post_checks(self, calc_date: date):
    #     # super().run_post_checks(calc_date)
    #     # super().run_metrics(calc_date)
    #     print(">> user post-checks")
