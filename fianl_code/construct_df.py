import findspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('lr_example').getOrCreate()
data = spark.read.csv("model_df.csv",inferSchema=True,header=True)
data.createOrReplaceTempView("df")
data = spark.sql("SELECT diff AS label, apartment, business, entertainment, food, government, hospital, locality, mall, park, sport, transportation, weekend, six_hour_1, six_hour_2, six_hour_3 FROM df")
