from datetime import date, timedelta

from pyspark.sql import DataFrame

from fs_etl.base_task import BaseEtlTask
from fs_etl.sql.sql_processor import SqlProcessor


class TransactionsCashflow(BaseEtlTask):
    """Расчет признаков по карточным транзакциям клиента"""
    def get_logical_date(self, scheduled_date: date) -> date:
        """
        Переопределение даты расчёта на T-1 от scheduled_date
        """
        scheduled_date = scheduled_date - timedelta(days=1)
        return scheduled_date

    def create_empty_table(self, tablename: str, df: DataFrame, partition_col: str = None):
        """Создает пустую таблицу в HDFS/Hive Metastore"""
        db, table = tablename.split('.')
        if_table_exists = self.spark_session._jsparkSession.catalog().tableExists(db, table)
        if not if_table_exists:
            if partition_col:
                df.where('1=0').write.format('hive').option('fileFormat', 'parquet') \
                .option('compression', 'snappy').mode('overwrite').partitionBy(partition_col) \
                .saveAsTable(tablename)
            else:
                df.where('1=0').write.format('hive').option('fileFormat', 'parquet') \
                .option('compression', 'snappy').mode('overwrite').saveAsTable(tablename)

    def process_data(self, logical_date: date):
        """
        Функция процессинга данных
        """
        target_table = self.settings.dest_settings.tables.get('transactions_cashflowcategory_stats').full_table_name
        repartition_num = self.settings.dest_settings.tables.get('transactions_cashflowcategory_stats').repartition_num
        sql_proc = SqlProcessor(self.spark_session, self.settings, logical_date)
        df = sql_proc.execute_sql_script('sql/transactions_cashflowcategory_stats.sql')
        self.create_empty_table(target_table, df, 'dt')
        schema = self.spark_session.read.table(target_table).columns
        df = df.select(schema)
        df = df.repartition(repartition_num)
        df.write.mode('overwrite').insertInto(target_table, overwrite=True)
        print('Success')
