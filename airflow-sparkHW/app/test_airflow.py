import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)
sc = spark.sparkContext
sc.setLogLevel("WARN")

csv_file = sys.argv[1]
output_file = sys.argv[2]

df_csv = (
    spark.read
        .format("csv")
        .option("header", True)
        .load(csv_file)
	)

df_csv.write.format('orc').save(output_file)
