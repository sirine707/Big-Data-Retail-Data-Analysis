import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType, LongType, TimestampType
import requests
import json
import os

if len(sys.argv) != 6:
    print("Usage: process_with_spark.py <kafka_bootstrap_servers> <kafka_topic> <output_dir> <es_host> <es_index>")
    sys.exit(1)

kafka_bootstrap_servers, kafka_topic, output_dir, es_host, es_index = sys.argv[1:6]

# Load schema from JSON file
schema_path = os.path.join(os.path.dirname(__file__), "../version/DVM_retail_data_v1.json")
with open(schema_path, "r", encoding="utf-8") as f:
    schema_json = json.load(f)

# Helper to map JSON types to Spark types
def json_type_to_spark_type(json_type):
    if json_type == "string":
        return StringType()
    if json_type in ["float", "double", "number"]:
        return FloatType()
    if json_type in ["integer", "int", "long"]:
        return LongType()
    if json_type == "datetime":
        return StringType()  # Use StringType for datetime, parse later if needed
    return StringType()

# Build StructType from JSON schema
fields = []
for name, typ in schema_json["columns"].items():
    spark_type = json_type_to_spark_type(typ.lower())
    fields.append(StructField(name, spark_type, True))
schema = StructType(fields)

spark = SparkSession.builder.appName("KafkaSparkProcessingES").getOrCreate()

# Read from Kafka
df = spark.read.format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", kafka_topic) \
    .load()

# Parse the JSON from the value column
df_json = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

print("=== DataFrame after JSON parsing ===")
df_json.show(truncate=False)
df_json.printSchema()


# Example aggregation: average unit_price grouped by product_category
if "unit_price" in df_json.columns and "product_category" in df_json.columns:
    df_agg = df_json.groupBy("product_category").agg(avg(col("unit_price")).alias("avg_unit_price"))
    print("=== Aggregated DataFrame ===")
    df_agg.show()
    df_agg.write.mode("overwrite").csv(output_dir)
    # Index aggregated rows in Elasticsearch
    for row in df_agg.collect():
        doc = row.asDict()
        requests.post(f"{es_host}/{es_index}/_doc", headers={"Content-Type": "application/json"}, data=json.dumps(doc))

# Index raw rows in Elasticsearch
for row in df_json.collect():
    doc = row.asDict()
    requests.post(f"{es_host}/{es_index}_raw/_doc", headers={"Content-Type": "application/json"}, data=json.dumps(doc))

spark.stop()