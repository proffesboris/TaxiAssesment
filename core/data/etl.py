from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DoubleType, LongType, TimestampNTZType
from pyspark.sql.functions import col
import os

def sum(arg):
    total = 0
    for val in arg:
        total += val
    return total


class TransformData:

    def set_schema(self):

        return StructType([
            StructField("VendorID", LongType(), True),
            StructField("tpep_pickup_datetime", TimestampNTZType(), True),
            StructField("tpep_dropoff_datetime", TimestampNTZType(), True),
            StructField("passenger_count", DoubleType(), True),
            StructField("trip_distance", DoubleType(), True),
            StructField("RatecodeID", DoubleType(), True),
            StructField("store_and_fwd_flag", StringType(), True),
            StructField("PULocationID", LongType(), True),
            StructField("DOLocationID", LongType(), True),
            StructField("payment_type", LongType(), True),
            StructField("fare_amount", DoubleType(), True),
            StructField("extra", DoubleType(), True),
            StructField("mta_tax", DoubleType(), True),
            StructField("tip_amount", DoubleType(), True),
            StructField("tolls_amount", DoubleType(), True),
            StructField("improvement_surcharge", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("congestion_surcharge", DoubleType(), True),
            StructField("airport_fee", DoubleType(), True)
        ])

    def merge_frames(self, sc):

        import glob

        import os

        root_path = os.path.join('core', 'temp', 'data')

        files = glob.glob(f"{root_path}/*")

        list_of_frames = []

        for i in files:
            list_of_frames.append(sc.read.parquet(i))

        from functools import reduce
        from pyspark.sql import DataFrame

        def unionAll(*dfs):
            return reduce(DataFrame.unionAll, dfs)

        return unionAll(*list_of_frames)


    def make_spark_context(self):

        spark = SparkSession.builder.master("local[*]").appName("Transform taxi data").getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        return spark


    def read_data(self):

        import os
        if len(os.listdir(os.path.join('core', 'temp', 'data'))) == 0:
            print("\n ------Directory is empty, please set up existing date/dates------ \n")
            exit(0)

        else:

            print("-------------------------------------------")
            print("\n------transform data in spark----------\n")
            print("-------------------------------------------")

            return self.merge_frames(sc=self.make_spark_context())


    def enrich_data(self):
        '''
        Replace all values lower than 0 in column total_amount
        '''

        from pyspark.sql.functions import when

        df = self.read_data()

        return df.withColumn("total_amount", when(df["total_amount"] < 0, 0).otherwise(df["total_amount"]))


    def get_10_percent_data_based_on_distance(self):
        '''
        Select top 10 percent of longest trips
        '''

        df = self.enrich_data()

        from pyspark.sql.window import Window
        from pyspark.sql.functions import percent_rank, col

        # print(df.select("trip_distance").rdd.max()[0])

        window = Window.partitionBy().orderBy(df['trip_distance'].desc())

        return (df.select('*', percent_rank().over(window).alias('rank'))
          .filter(col('rank') <= 0.1)
          .orderBy(df['trip_distance'], ascending=False))

    def save_results_on_disk(self):

        self.get_10_percent_data_based_on_distance().coalesce(1).write.mode('overwrite').csv('result', header=True)

        print("-------------------------------------------")
        print("\n------Finished----------\n")
        print("-------------------------------------------")