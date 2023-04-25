
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, split, explode
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql import SparkSession

# Create Spark session and Glue context
sc = SparkContext()
spark = SparkSession.builder \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
glueContext = GlueContext(sc)

# Create data schema
schema = StructType([
    StructField("product_name", StringType(), True),
    StructField("product_price", IntegerType(), True)
])

# Read data from S3 bucket
input_bucket = 'your input bucket name'
raw_data = glueContext.create_dynamic_frame.from_options(connection_type="s3",
                                                         connection_options={
                                                             "path": f"s3://{input_bucket}/raw_data.csv"
                                                         },
                                                         format="csv",
                                                         format_options={
                                                             "header": True,
                                                             "delimiter": ","
                                                         },
                                                         transformation_ctx="raw_data").toDF()

# Split product name that is separated by '-'
split_col = split(raw_data['product_name'], '-')
# Create new columns from the split values
raw_data = raw_data.withColumn('product_id', split_col.getItem(0))
raw_data = raw_data.withColumn('product_description', split_col.getItem(1))

# Explode product price that is separated by ','
raw_data = raw_data.withColumn('product_price',
                               explode(split(col('product_price'), ','))).withColumn('product_price',
                                                                                       col('product_price').cast(
                                                                                           IntegerType()))

# Filter out products that are more than $500
raw_data = raw_data.filter(col('product_price') <= 500)

# Write data to Athena
output_bucket = 'your output bucket name'
database_name = 'your database name'
table_name = 'your table name'
raw_data.write \
    .format("csv") \
    .option("delimiter", ",") \
    .mode("append") \
    .bucketBy(100, "product_id") \
    .option("path", f's3://{output_bucket}/{table_name}/') \
    .option("checkpointLocation", f's3://{output_bucket}/checkpoints/') \
    .saveAsTable(f'{database_name}.{table_name}')
