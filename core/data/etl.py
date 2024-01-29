from pyspark.sql import SparkSession

class TransformData:

    def __init__(self, data_path=None):
        self.data_path = data_path

    def __path_for_read(self):

        import os


        if self.data_path is None:
            path_for_read_files = os.path.join('core', 'temp', 'data')
        else:
            path_for_read_files = self.data_path

        return path_for_read_files



    def __merge_frames(self, sc, root_path):

        import glob

        files = glob.glob(f"{root_path}/*")

        list_of_frames = []

        for i in files:
            list_of_frames.append(sc.read.parquet(i))

        from functools import reduce
        from pyspark.sql import DataFrame

        def unionAll(*dfs):
            return reduce(DataFrame.unionAll, dfs)

        return unionAll(*list_of_frames)


    def __make_spark_context(self):

        spark = SparkSession.builder.master("local[*]").appName("Transform taxi data").getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        return spark


    def __read_data(self):

        import os
        if len(os.listdir(os.path.join('core', 'temp', 'data'))) == 0:
            print("\n ------Directory is empty, please set up existing date/dates------ \n")
            exit(0)

        else:

            print("-------------------------------------------")
            print("\n------transform data in spark----------\n")
            print("-------------------------------------------")

        return self.__merge_frames(sc=self.__make_spark_context(), root_path=self.__path_for_read())


    def enrich_data(self):
        '''
        Replace all values lower than 0 in column total_amount
        '''

        from pyspark.sql.functions import when

        df = self.__read_data()

        return df.withColumn("total_amount", when(df["total_amount"] < 0, 0).otherwise(df["total_amount"]))


    def get_10_percent_data_based_on_distance(self):
        '''
        Select top 10 percent of longest trips
        '''

        df = self.enrich_data()

        from pyspark.sql.window import Window
        from pyspark.sql.functions import percent_rank, col

        window = Window.partitionBy().orderBy(df['trip_distance'].desc())

        return (df.select('*', percent_rank().over(window).alias('rank'))
          .filter(col('rank') <= 0.1)
          .orderBy(df['trip_distance'], ascending=False))

    def save_results_on_disk(self):

        self.get_10_percent_data_based_on_distance().coalesce(1).write.mode('overwrite').csv('result', header=True)

        print("-------------------------------------------")
        print("\n------Finished----------\n")
        print("-------------------------------------------")