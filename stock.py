#!/home/vasanthan/PycharmProjects/hadoop/venv/bin/spark-submit
from pyspark.sql import SparkSession
import os
import matplotlib.pyplot as plt


class Stock(object):

    def __init__(self):
        data_csv_path_8th_jan = os.path.join(os.path.dirname(__file__), 'resources/cm08JAN2019bhav.csv')
        data_csv_path_1th_jan = os.path.join(os.path.dirname(__file__), 'resources/cm01JAN2019bhav.csv')
        self.spark = SparkSession.builder.appName("stock Stat").getOrCreate()
        self.data_frame = self.spark.read.option('header', 'true').option('inferSchema', 'true').csv(
            "file://" + data_csv_path_8th_jan)
        self.join_data_frame = self.spark.read.option('header', 'true').option('inferSchema', 'true').csv(
            "file://" + data_csv_path_1th_jan).withColumnRenamed('OPEN', 'jan1_open').withColumnRenamed('TOTALTRADES',
                                                                                                        'TOTALTRADES1J')

    def get_top_in_market(self):
        ax = plt.gca()
        self.data_frame.sort('OPEN', ascending=False).limit(10).toPandas().plot(kind='bar', x='SYMBOL', y='OPEN', ax=ax)
        # plt.show()

    def compare_business_between_interval(self):
        new_df = self.data_frame.join(self.join_data_frame, (self.data_frame.SYMBOL == self.join_data_frame.SYMBOL),
                                      'inner') \
            .select(self.data_frame.SYMBOL, self.data_frame.OPEN, self.join_data_frame.jan1_open) \
            .sort('OPEN', ascending=False).limit(10).toPandas()
        fig, ax = plt.subplots(figsize=(15, 8))
        new_df.plot(y=['jan1_open'], kind='line', ax=ax)
        new_df.plot(x='SYMBOL', y=['OPEN', 'jan1_open'], kind='bar', ax=ax)
        # plt.show()

    def top_trades(self):
        ax = plt.gca()
        self.data_frame.sort('TOTALTRADES', ascending=False).limit(10).toPandas().plot(kind='bar', x='SYMBOL',
                                                                                       y='TOTALTRADES', ax=ax)
        # plt.show()

    def compare_trades_between_points(self):
        new_df = self.data_frame.join(self.join_data_frame, (self.data_frame.SYMBOL == self.join_data_frame.SYMBOL),
                                      'inner') \
            .select(self.data_frame.SYMBOL, self.data_frame.TOTALTRADES, self.join_data_frame.TOTALTRADES1J) \
            .sort('TOTALTRADES', ascending=False).limit(10).toPandas()
        fig, ax = plt.subplots(figsize=(15, 8))
        new_df.plot(y=['TOTALTRADES1J'], kind='line', ax=ax)
        new_df.plot(x='SYMBOL', y=['TOTALTRADES', 'TOTALTRADES1J'], kind='bar', ax=ax)
        # plt.show()

    def grp_key(self):
        self.data_frame


if __name__ == '__main__':
    s = Stock()
    s.get_top_in_market()
    s.compare_business_between_interval()
    s.top_trades()
    s.compare_trades_between_points()
    plt.show()
