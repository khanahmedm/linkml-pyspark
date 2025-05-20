import os
os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.hadoop.hadoop.native.io=false --packages io.delta:delta-core_2.12:2.4.0 pyspark-shell"

from pyspark.sql import SparkSession

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("Read Delta Table") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.hadoop.native.io", "false") \
    .getOrCreate()

# Path to the Delta table
delta_path = "file:///C:/tmp/delta/products"

# Read Delta table
df = spark.read.format("delta").load(delta_path)

# Show results
df.show()

spark.stop()
