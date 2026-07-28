from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    split,
    col,
    when,
    date_format,
    regexp_replace,
    concat,
    to_date,
    lpad,
    bround,
    lit,
)
from pyspark.sql.types import DoubleType

spark = (
    SparkSession.builder.appName("KafkaSparkStreaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3")
    .config("spark.sql.streaming.streamingTimeout", "600000")
    .getOrCreate()
)
spark.conf.set("spark.sql.streaming.trigger.interval", "2s")  # Adjust trigger interval
spark.conf.set(
    "spark.streaming.backpressure.enabled", "true"
)  # Enable backpressure when needed
spark.conf.set(
    "spark.streaming.kafka.maxRatePerPartition", "1000"
)  # Limit Kafka consumption rate
spark.conf.set("spark.sql.shuffle.partitions", "1")

# Read data from Kafka
kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "test")
    .option("startingOffsets", "latest")
    .load()
)

csv_df = kafka_df.selectExpr("CAST(value AS STRING)")

# Split CSV data into columns
processed_df = csv_df.select(split(csv_df["value"], ",").alias("columns"))

# Convert to individual columns
final_df = processed_df.selectExpr(
    "columns[0] AS User",
    "columns[1] AS Card",
    "columns[2] AS Year",
    "columns[3] AS Month",
    "columns[4] AS Day",
    "columns[5] AS Time",
    "columns[6] AS Amount",
    "columns[7] AS UseChip",
    "columns[8] AS MerchantName",
    "columns[9] AS MerchantCity",
    "columns[10] AS MerchantState",
    "columns[11] AS Zip",
    "columns[12] AS MCC",
    "columns[13] AS Errors",
    "columns[14] AS IsFraud",
)

# Filter out fraudulent transactions
final_df_filtered = final_df.filter(final_df["IsFraud"] == "No")

# Convert the amount to VND (assuming 1 USD = 25,370 VND)
final_df_filtered = final_df_filtered.withColumn(
    "Amount",
    bround((regexp_replace(col("Amount"), "[$,]", "").cast(DoubleType()) * 25370), 2),
)

# Format date and time to the required pattern (dd/mm/yyyy and hh:mm:ss)
final_df_filtered = final_df_filtered.withColumn(
    "Time",
    concat(
        lpad(col("Time"), 5, "0"),  # Left-pad hour and minute values
        lit(":00"),  # Append seconds
    ),
)
final_df_filtered = final_df_filtered.withColumn(
    "Date",
    date_format(
        to_date(
            concat(
                lpad(col("Day").cast("string"), 2, "0"),  # Ensure day has two digits
                lit("/"),
                lpad(
                    col("Month").cast("string"), 2, "0"
                ),  # Ensure month has two digits
                lit("/"),
                col("Year").cast("string"),
            ),
            "dd/MM/yyyy",
        ),
        "dd/MM/yyyy",  # Ensure the final format is dd/MM/yyyy
    ),
)

# Write the results to HDFS or another distributed file system
try:
    query = (
        final_df_filtered.coalesce(1)
        .writeStream.outputMode("append")
        .format("csv")
        .option("path", "/credit_card_output")
        .option("checkpointLocation", "/credit_card_checkpoint")
        .start()
    )

    query.awaitTermination(60)

except Exception as e:
    print(f"An error occurred: {e}")
    query.awaitTermination()

finally:
    query.stop()
