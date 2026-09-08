from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os
from dotenv import load_dotenv

#Load environment variables
load_dotenv()

# --- Config ---
KAFKA_TOPIC = "tracking_topic"
KAFKA_BOOTSTRAP = "localhost:9092"

MYSQL_DATABASE =os.getenv("MYSQL_DATABASE")
MYSQL_JDBC_URL = f"jdbc:mysql://localhost:3307/{MYSQL_DATABASE}"
MYSQL_PROPS = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

def create_spark_session():
    return (
        SparkSession.builder
        .appName("KafkaSparkStreaming")
        # We need the Kafka SQL library now
        .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1,com.mysql:mysql-connector-j:8.0.33').getOrCreate()
    )
    
# --- The Function to Write Each Batch to MySQL ---
def write_to_mysql(batch_df, batch_id):
    """
    This function is called for every small 'batch' of data 
    that comes through the stream.
    """
    
    print(f"Processing Batch ID: {batch_id}")
    if batch_df.isEmpty():
        return
    
    # 1. Perform the Heavy Transformation (Pivot/Agg)
    # We replicate your logic here, but on the micro-batch
    
    df_with_time = batch_df.withColumn('dates', F.date_trunc('day', F.col('ts')))\
                            .withColumn('hours', F.hour(F.col('ts')))
    dimensions = ['dates', 'hours', 'job_id', 'publisher_id', 'campaign_id', 'group_id']
    
    pivot_df = (
        df_with_time.groupBy(*dimensions)
        .pivot('custom_track', ['click', 'conversion', 'qualified', 'unqualified'])
        .agg(
            F.count(F.col('custom_track')).alias('count'),
            F.sum(F.col('bid')).alias('spend'),
            F.avg(F.col('bid')).alias('avg_bid')
        )
    )
    
    # Clean up columns to match MySQL Schema
    final_df = pivot_df.select(
        *dimensions,
        F.round(F.col('click_avg_bid'), 2).alias('bid_set'),
        F.col('click_spend').alias('spend_hour'),
        F.col('click_count').alias('clicks'),
        F.col('conversion_count').alias('conversion'),
        F.col('qualified_count').alias('qualified_application'),
        F.col('unqualified_count').alias('disqualified_application'),
        F.lit('Kafka').alias('sources') # Mark source as Kafka
    ).fillna(0) 
    
    # Fill nulls with 0 for calculations
    final_df = final_df.withColumn("updated_at", F.current_timestamp())
    
     # 2. Write to MySQL
    print(f"Writing {final_df.count()} rows to MySQL...")
    (
        final_df.write.format("jdbc")
        .option("url", MYSQL_JDBC_URL)
        .option("driver", MYSQL_PROPS["driver"])
        .option("dbtable", "events")
        .mode("append")
        .option("user", MYSQL_PROPS["user"])
        .option("password", MYSQL_PROPS["password"])
        .save()
    )
    
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Read from Kafka (The Stream)
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest") # Or 'earliest' to re-process everything
        .load()
    )

    # 2. Parse JSON Data
    # Kafka sends data as bytes in a 'value' column. We must cast to String -> JSON.
    schema = StructType([
        StructField("created_at", StringType(), True),
        StructField("job_id", IntegerType(), True),
        StructField("custom_track", StringType(), True),
        StructField("bid", DoubleType(), True),
        StructField("campaign_id", IntegerType(), True),
        StructField("group_id", IntegerType(), True),
        StructField("publisher_id", IntegerType(), True)
    ])

    json_df = kafka_df.select(
        F.from_json(F.col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    processed_stream = json_df.withColumn('ts', F.to_timestamp('created_at',"yyyy-MM-dd HH:mm:ss"))

    # 4. Start the Stream
    # We use 'foreachBatch' to handle the complex aggregation and MySQL write
    query = (
        processed_stream.writeStream
        .foreachBatch(write_to_mysql)
        .outputMode("append") # 'update' or 'append' depending on aggregation needs
        .start()
    )

    print("Streaming started... Waiting for data...")
    query.awaitTermination()

if __name__ == "__main__":
    main()