from pyspark.sql import SparkSession
import os

class Data:
    def __init__(self):
        self.data = None
        self.spark = SparkSession.builder.appName("DataLoader").getOrCreate()


    def read_csv(self, filename):
        main_dir = os.path.dirname(os.path.abspath(__file__))
        resources_dir = os.path.join(main_dir, '../resources')
        filepath = os.path.join(resources_dir, 'data', filename)

        self.data = self.spark.read.csv(filepath, header=True, inferSchema=True)
        self.data = self.data.toDF(*[c.lower() for c in self.data.columns])        
