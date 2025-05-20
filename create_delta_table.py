# parse the schema

from linkml_runtime.utils.schemaview import SchemaView
from linkml_runtime.loaders import yaml_loader

sv = SchemaView("schema.yaml")
product_class = sv.get_class("Product")
slots = sv.class_slots("Product")

print(product_class)
print(slots)

from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Convert the LinkML slots to PySpark types

def map_range_to_spark_type(slot_range):
    if slot_range == "string":
        return StringType()
    elif slot_range == "float":
        return FloatType()
    # Add more mappings as needed
    else:
        return StringType()  # default fallback

fields = [
    StructField(slot.name, map_range_to_spark_type(slot.range), True)
    for slot in [sv.induced_slot(s, "Product") for s in slots]
]

product_schema = StructType(fields)

print(product_schema)

# Create Delta Table with PySpark
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("LinkML to Delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Delta Example") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.hadoop.native.io", "false") \
    .getOrCreate()

# Sample data
data = [("p001", "Widget", 19.99)]
df = spark.createDataFrame(data, schema=product_schema)

# Save as Delta table
df.write.format("delta").mode("overwrite").save("C:/tmp/delta/products")



