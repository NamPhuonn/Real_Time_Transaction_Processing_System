from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)

# Create a SparkSession with Hive support
spark = (
    SparkSession.builder.appName("MergeCSVWithSchema").enableHiveSupport().getOrCreate()
)

# Define the schema
schema = StructType(
    [
        StructField("User", IntegerType(), True),
        StructField("Card", IntegerType(), True),
        StructField("Year", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Day", IntegerType(), True),
        StructField("Time", StringType(), True),
        StructField("Amount", FloatType(), True),
        StructField("Use Chip", StringType(), True),
        StructField("Merchant Name", StringType(), True),
        StructField("Merchant City", StringType(), True),
        StructField("Merchant State", StringType(), True),
        StructField("Zip", FloatType(), True),
        StructField("MCC", StringType(), True),
        StructField("Errors?", StringType(), True),
        StructField("Is Fraud?", StringType(), True),
        StructField("Date", StringType(), True),
    ]
)

# Input and output paths
input_path = "/credit_card_output"  # Folder containing CSV files
output_path = "/merged_credit_card_output"  # Folder for the merged output

# Read the data using the defined schema
all_csv_df = spark.read.csv(
    input_path, schema=schema, header=False
)  # header=False because the files have no header

# Write the data into a single file
all_csv_df.coalesce(1).write.csv(output_path, header=True, mode="overwrite")

print(f"Data merged and saved to Hive table: {output_path}")
