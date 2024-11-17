from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, avg, count, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType, LongType
import logging
import json
import mysql.connector
from mysql.connector import Error
import time

# Initialize logging for better debugging and observability
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.info("Starting Spark job to read from Kafka...")

def create_spark_session():
    """
    Create and configure a Spark session with required dependencies and settings.
    - Includes Kafka and MySQL connectors.
    - Configures checkpointing for stateful operations.
    """
    
    logger.info("Creating Spark session...")
    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.kafka:kafka-clients:3.5.1",
        "mysql:mysql-connector-java:8.0.33" 
    ]
    
    spark = (SparkSession.builder
            .appName("ECommerceProcessor")
            .config("spark.jars.packages", ",".join(packages))
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.driver.extraClassPath", "/opt/bitnami/spark/jars/*")
            .config("spark.executor.extraClassPath", "/opt/bitnami/spark/jars/*")
            .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def define_schema():
    """
    Define the schema of the JSON data consumed from Kafka.
    - Ensures proper parsing and type validation.
    """
    
    schema = StructType([
        StructField("category", StringType(), True),
        StructField("location", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("timestamp", LongType(), True),
        StructField("transaction_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True)
    ])
    logger.info(f"Schema defined: {schema.json()}")
    return schema

def read_from_kafka(spark):
    """
    Read streaming data from Kafka topic.
    - Connects to the `ecommerce-transactions` topic.
    - Reads messages from the earliest offset.
    """
    
    try:
        logger.info("Setting up Kafka stream reader...")

        df = (spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", "kafka:9092")
              .option("subscribe", "ecommerce-transactions")
              .option("startingOffsets", "earliest")
              .load())
        
        logger.info("Kafka stream reader setup successful.")
        logger.info(f"Kafka input schema: {df.schema.json()}")
        
        logger.info("Spark is now ready to process messages from Kafka.")
        
        return df
    except Exception as e:
        logger.error(f"Error connecting to Kafka: {str(e)}", exc_info=True)
        raise


def process_kafka_data(df, schema):
    """
    Process Kafka data by:
    - Parsing JSON payloads.
    - Converting Unix timestamps to human-readable format.
    - Calculating aggregate metrics (e.g., total sales, average price).
    """
    
    try:
        # Parse JSON from Kafka value field
        parsed_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")
        
        # Add a timestamp column
        parsed_df = parsed_df.withColumn(
            "event_timestamp", 
            from_unixtime(col("timestamp")).cast(TimestampType())
        )
        
        # Aggregate data using windowed processing
        result_df = (parsed_df
                .withWatermark("event_timestamp", "1 minute")
                .groupBy(
                    window("event_timestamp", "1 minute"),
                    "category"
                )
                .agg(
                    sum(col("price") * col("quantity")).alias("total_sales"),
                    avg("price").alias("avg_price"),
                    sum("quantity").alias("total_quantity"),
                    count("*").alias("num_transactions")
                )
                .select(
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    "category",
                    "total_sales",
                    "avg_price",
                    "total_quantity",
                    "num_transactions"
                ))
        
        logger.info(f"Result schema: {result_df.schema.json()}")
        return result_df
    
    except Exception as e:
        logger.error(f"Error in process_kafka_data: {str(e)}", exc_info=True)
        raise

def write_to_mysql(df, epoch_id):
    """
    Write processed batch data to MySQL.
    - Appends data to the `sales_analytics` table.
    - Uses JDBC for database connectivity.
    """
    
    try:
        logger.info(f"Writing batch {epoch_id} to MySQL...")
        count = df.count()
        logger.info(f"Number of records in batch: {count}")
        
        if count > 0:
            logger.info("Data to be written:")
            df.show(truncate=False)
            
            (df.write
             .format("jdbc")
             .mode("append")
             .option("driver", "com.mysql.cj.jdbc.Driver")
             .option("url", "jdbc:mysql://mysql:3306/ecommerce")
             .option("dbtable", "sales_analytics")
             .option("user", "spark")
             .option("password", "spark123")
             .option("batchsize", "1000")
             .option("truncate", "false")
             .save())
            
            logger.info(f"Successfully wrote batch {epoch_id} to MySQL")
        else:
            logger.warning(f"Batch {epoch_id} had no records to write")
            
    except Exception as e:
        logger.error(f"Error writing to MySQL: {str(e)}", exc_info=True)
        raise

def verify_mysql_connection(max_retries=5, delay_seconds=5):
    """
    Test MySQL connectivity with retries.
    - Verifies the database is reachable before starting the main job.
    """
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting MySQL connection (attempt {attempt + 1}/{max_retries})...")
            conn = mysql.connector.connect(
                host="mysql",
                database="ecommerce",
                user="spark",
                password="spark123"
            )
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            logger.info("MySQL connection test successful")
            return True
        except mysql.connector.Error as e:
            logger.warning(f"MySQL connection attempt {attempt + 1} failed: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Waiting {delay_seconds} seconds before next attempt...")
                time.sleep(delay_seconds)
            else:
                logger.error("All MySQL connection attempts failed")
                raise
        finally:
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals() and conn.is_connected():
                    conn.close()
            except:
                pass

def main():
    """
    Main function to orchestrate the Spark job:
    - Verify MySQL connectivity.
    - Create Spark session and read from Kafka.
    - Process and write data to MySQL.
    """
    
    try:
        verify_mysql_connection()
        spark = create_spark_session()
        schema = define_schema()
        kafka_df = read_from_kafka(spark)
        processed_df = process_kafka_data(kafka_df, schema)
        
        # Write to console for debugging
        console_query = (processed_df
                        .writeStream
                        .outputMode("append")
                        .format("console")
                        .trigger(processingTime='10 seconds')  # Add explicit trigger
                        .start())
        
        # Write to MySQL
        mysql_query = (processed_df
                      .writeStream
                      .outputMode("append")
                      .foreachBatch(write_to_mysql)
                      .trigger(processingTime='10 seconds')  # Add explicit trigger
                      .start())
        
        logger.info("All streams started successfully")
        
        # Wait for termination
        spark.streams.awaitAnyTermination()
        
    except Exception as e:
        logger.error(f"Error in main: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()