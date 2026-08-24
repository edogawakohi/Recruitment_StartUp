import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType
from cassandra.util import datetime_from_uuid1
from uuid import UUID
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

#Load environment variables
load_dotenv()

os.environ["PYSPARK_PYTHON"] = r"C:\Users\PC\miniconda3\envs\recruitment\python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\PC\miniconda3\envs\recruitment\python.exe"

#Configuartion
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE")
CASSANDRA_TABLE = os.getenv("CASSANDRA_TABLE")

MYSQL_DATABASE =os.getenv("MYSQL_DATABASE")
MYSQL_TARGET_TABLE = os.getenv("MYSQL_TARGET_TABLE")

JDBC_PROPERTIES ={
    "url": f"jdbc:mysql://mysql/{MYSQL_DATABASE}?connectionTimeZone=Asia/Ho_Chi_Minh&forceConnectionTimeZoneToSession=true",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD")
}

print("MYSQL_DATABASE =", MYSQL_DATABASE)
print("JDBC_URL =", JDBC_PROPERTIES["url"])
print("MYSQL_USER =", JDBC_PROPERTIES["user"])
print("MYSQL_PASSWORD =", JDBC_PROPERTIES["password"])

# @udf(returnType=TimestampType())
# def timeuuid_todatetime(uuid_str):
#     """Convert a TimeUUID string to a datetime.datetime object."""
#     try:
#         # Assumes create_time is a string representation of a UUID
#         return datetime_from_uuid1(UUID(uuid_str))
#     except (TypeError, ValueError, AttributeError):
#         # Handle potential nulls or malformed data
#         return None

def create_spark_session():
    """Initialize and returns a SparkSession"""
    # Connector need to match your Spark's Scala version
    cassandra_connector = "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
    mysql_connnector = "mysql:mysql-connector-java:8.0.33"

    return(
        SparkSession.builder
        .appName("Cassandra to MYSQL ELT Pipeline")
        .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        .config("spark.jars.packages", f"{cassandra_connector},{mysql_connnector}")
        .config("spark.cassandra.connection.host", "cassandra")
        .getOrCreate()
    )

def extract_from_cassandra(spark, keyspace, table, last_processed_time):
    """
    Extracts data from Cassandra, applies the UDF to get the timestamp,
    and then filters for new data in Spark.
    """
    print(f"Extracting data from Cassandra keyspace: {keyspace}, table: {table}")

    #1. Load All data from Cassandra (creates the first "temp table")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    
    raw_df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=table, keyspace=keyspace)
        .load()
    )

    df_with_ts = raw_df.withColumn('ts', col('created_at')- F.expr("INTERVAL 7 HOURS"))

    filtered_df = df_with_ts.where(
        F.col("ts") > last_processed_time
    )
 
    #2. Transform 'create_time' to "ts" (creates the second "temp table")
    df_with_ts = raw_df.withColumn('ts', col('created_at') - F.expr("INTERVAL 7 HOURS"))

    #3.Filter (compare) using the new 'ts' (creates the second "temp table")
    print(f"Filtering for data newer than: {last_processed_time}")
    filtered_df = df_with_ts.where(col('ts') > last_processed_time)
    return filtered_df

def extract_from_mysql(spark, jdbc_props, query):
    """Extracts data from MySQL using a JDBC connection and query."""
    print(f"Extracting data from MySQL: {query}")
    return (
        spark.read.format('jdbc')
        .option("url", jdbc_props["url"])
        .option("driver", jdbc_props["driver"])
        .option("dbtable", query)
        .option("user", jdbc_props["user"])
        .option("password", jdbc_props["password"])
        .load()
    )

def process_data(df):
    """
    Converts the TimeUUID column to a standard timestamp and selects
    the necessary columns for transformation.
    """
    print("Processing raw data: Converting TimeUUID...")

    # The UDF is now global, so we just call it
    df_with_ts = df.withColumn('ts', col('created_at')- F.expr("INTERVAL 7 HOURS"))

    # Select only the columns needed for the next step
    return df_with_ts.select(
        'ts',
        'job_id',
        'custom_track',
        'bid',
        'campaign_id',
        'group_id',
        'publisher_id'
    )

def transform_data(df, jobs_df, updated_at_val):
    """
    Pivots and aggregates tracking data, then enriches it with jobs data.
    
    This replaces the four separate filters, temp views, SQL queries,
    and full joins with a single, efficient pivot operation.
    """
    print("Transforming data: Pivoting and aggregating...")

    #Add date and hour columns for grouping
    df_with_timeparts = df.withColumn('dates', F.date_trunc('day', col('ts'))) \
                        .withColumn('hours', F.hour(col('ts')))
    
    dimensions = ['dates', 'hours', 'job_id', 'publisher_id', 'campaign_id', 'group_id']

    # Perform a single pivot and aggregation
    pivot_df = (
        df_with_timeparts.groupBy(*dimensions)
        .pivot('custom_track', ['click', 'conversion', 'qualified', 'unqualified'])
        .agg(
            F.count(col('custom_track')).alias('count'),
            F.sum(col('bid')).alias('spend'),
            F.avg(col('bid')).alias('avg_bid')
        )
    )

    # Filter out any rows where date is null (as in original script)
    pivot_df = pivot_df.filter(col('dates').isNotNull())

    # Rename columns to match the desired final schema
    final_df = pivot_df.select(
        *dimensions,
        F.round(col('click_avg_bid'), 2).alias('bid_set'),
        col('click_spend').alias('spend_hour'),
        col('click_count').alias('clicks'),
        col('conversion_count').alias('conversion'),
        col('qualified_count').alias('qualified_application'),
        col('unqualified_count').alias('disqualified_application')
    )

    print("Enriching data with jobs information...")
    # Join with jobs data (enrichment)
    final_df = final_df.join(jobs_df, on='job_id', how='left')\
                        .drop(jobs_df.campaign_id) \
                        .drop(jobs_df.group_id)

    # Add metadata columns
    final_df = final_df.withColumn('updated_at',F.lit(updated_at_val))
    final_df = final_df.withColumn('sources', F.lit('Cassandra'))

    return final_df

def load_to_mysql(df, jdbc_props, table_name):
    print(f"Loading data to MySQL table: {table_name}")

    (
        df.write.format("jdbc")
        .option("driver", jdbc_props["driver"])
        .option("url", jdbc_props["url"])
        .option("dbtable", table_name)
        .mode("append")
        .option("user", jdbc_props["user"])
        .option("password", jdbc_props["password"])
        .save()
    )


def get_latest_time_cassandra(spark):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking',keyspace = 'recruitment_startup').load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

def get_mysql_latest_time(spark, jdbc_props):    
    sql = """(select max(updated_at) from events) data"""
    mysql_time_df = spark.read.format('jdbc') \
        .option("url", jdbc_props["url"]) \
        .option("driver", jdbc_props["driver"]) \
        .option("dbtable", sql) \
        .option("user", jdbc_props["user"]) \
        .option("password", jdbc_props["password"]) \
        .option("connectionTimeZone", "Asia/Ho_Chi_Minh") \
        .option("forceConnectionTimeZoneToSession", "true") \
        .load()
    
    mysql_time = mysql_time_df.take(1)[0][0]
    
    if mysql_time is None:
        # Return a datetime object for the default start time
        return datetime.strptime('2022-07-26 15:58:36', '%Y-%m-%d %H:%M:%S')
    else:
        return mysql_time

def etl_flow(spark, new_data_df):
    """
    Runs the ETL process on the provided DataFrame of new data.
    Assumes new_data_df is cached.
    """
    JOBS_QUERY = "(SELECT id as job_id, company_id, group_id, campaign_id FROM job) A"

    try:
         # 1. EXTRACT (Load jobs data)
        jobs_df = extract_from_mysql(spark, JDBC_PROPERTIES, JOBS_QUERY)

        # 2. PROCESS (Light Transform)
        # Select the columns needed from the new data
        processed_data = new_data_df.select(
            'ts', 
            'job_id', 
            'custom_track', 
            'bid', 
            'campaign_id', 
            'group_id', 
            'publisher_id'
        )
        
        print("--- Sample Processed Data (New Data Only) ---")
        processed_data.show(5)

        # max_ts = processed_data.select(
        # F.date_format(
        #     F.max("ts"),
        #     "yyyy-MM-dd HH:mm:ss"
        # ).alias("max_ts")
        #  ).collect()[0]["max_ts"]

        # if max_ts:
        #     updated_at_timestamp = datetime.strptime(
        #         max_ts,
        #         "%Y-%m-%d %H:%M:%S"
        # )
        
        # Get max timestamp *from the new data*
        max_ts = processed_data.agg(F.max('ts')).collect()[0][0]
        if max_ts:
            # Round the time UP to the next second to avoid the infinite loop
            # This turns 14:30:16.123 -> 14:30:17
            updated_at_timestamp = (max_ts + timedelta(seconds=1)).replace(microsecond=0)
        else:
            updated_at_timestamp = datetime.now()
    
        # 3. TRANSFORM (Heavy Lifting)
        final_data = transform_data(processed_data, jobs_df, updated_at_timestamp)
        
        # 4. LOAD
        load_to_mysql(final_data, JDBC_PROPERTIES, MYSQL_TARGET_TABLE)
        print("load data")

    except Exception as e:
        # Log any errors during the transform/load
        print(f"An error occurred during the ETL process: {e}", file=sys.stderr)

def main():
    # 1. Create the Spark session ONCE, outside the loop
    spark = create_spark_session()
    # This will hide the Cassandra warnings and other clutter.
    spark.sparkContext.setLogLevel("ERROR")

    try:
        while True:
            start_time = datetime.now()
            raw_data_cached = None # Define here to use in the finally block
            
            try:
                # 1. Get the latest time we have successfully processed in MySQL
                mysql_latest_time = get_mysql_latest_time(spark, JDBC_PROPERTIES)
                print(f'Looking for new data since: {mysql_latest_time}')
                
                # 2. Extract new data from Cassandra
                raw_data = extract_from_cassandra(
                    spark, 
                    CASSANDRA_KEYSPACE, 
                    CASSANDRA_TABLE, 
                    mysql_latest_time
                )
                
                # 3. Cache the new data
                raw_data_cached = raw_data.cache()
                
                # 4. Perform the check in main
                if raw_data_cached.isEmpty():
                    print("No new data found in Cassandra after filtering.")
                else:
                    # 5. If data exists, pass the cached DataFrame to etl_flow
                    print("New data found, starting ETL process...")
                    etl_flow(spark, raw_data_cached) # Pass the DataFrame
                
            except Exception as e:
                # Catch errors inside the loop
                print(f"An error occurred during this run: {e}", file=sys.stderr)
            finally:
                # 6. Unpersist the cache after the run (or if it failed)
                if raw_data_cached:
                    raw_data_cached.unpersist()

            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            print(f'Job run finished. Execution time: {execution_time} seconds.')
            print('Sleeping for 5 seconds...')
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nShutting down job.")
    finally:
        # 2. Stop the Spark session ONCE, when the loop is finally broken
        if spark:
            spark.stop()
            print("Spark session stopped.")
if __name__ == "__main__":
    main()