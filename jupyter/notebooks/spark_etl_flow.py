#!/usr/bin/env python3
"""
Spark Streaming ELT Pipeline for GlobalMart

This pipeline implements a three-stream data flow:
1. Extract: Read streaming data from Kafka topics
2. Transform: Parse JSON and validate data using Spark DataFrame operations
3. Load (Three streams):
   a. Invalid data -> Console/File audit for manual review
   b. Valid data -> Process (stub) -> Kafka processed topics
   c. Valid data -> Process (stub) -> MongoDB star schema (via PyMongo)

All processing is done using Spark DataFrame operations on the cluster.
MongoDB writes use PyMongo directly to avoid Spark connector version issues.

Topics consumed:
- new_users
- new_transactions
- new_products
- new_sessions

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_etl_flow.py
    Or run directly in Jupyter with PySpark kernel
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp, 
    date_format, year, month, dayofmonth, hour, dayofweek, quarter,
    size, when, lit, concat_ws, concat, lpad, udf, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, ArrayType, TimestampType, DoubleType
)
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import uuid
from datetime import datetime

# Configuration
KAFKA_BROKERS = "kafka1:9092,kafka2:9092"

# MongoDB Configuration
MONGODB_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
MONGODB_DATABASE = "globalmart"

# Kafka topics - source
TOPICS = {
    'users': 'new_users',
    'products': 'new_products',
    'transactions': 'new_transactions',
    'sessions': 'new_sessions'
}

# Kafka topics - processed output
PROCESSED_TOPICS = {
    'users': 'processed_users',
    'products': 'processed_products',
    'transactions': 'processed_transactions',
    'sessions': 'processed_sessions'
}

# Local checkpoint path (no HDFS)
CHECKPOINT_PATH = "/tmp/spark_checkpoints"


def create_spark_session():
    """Create and configure Spark session."""
    spark = SparkSession.builder \
        .appName("GlobalMart-ELT-Pipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✓ Spark session created")
    return spark


# Define schemas for each topic
def get_user_schema():
    """Schema for user records."""
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("registeration_date", StringType(), True)  # Note: typo from original
    ])

def get_product_schema():
    """Schema for product records."""
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("inventory", IntegerType(), True),
        StructField("ratings", FloatType(), True)
    ])

def get_transaction_product_schema():
    """Schema for products array within transaction."""
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", FloatType(), True)
    ])

def get_transaction_schema():
    """Schema for transaction records."""
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("products", ArrayType(get_transaction_product_schema()), True),
        StructField("payment_method", StringType(), True)
    ])

def get_session_event_schema():
    """Schema for events array within session."""
    return StructType([
        StructField("eventType", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def get_session_schema():
    """Schema for session records."""
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("events", ArrayType(get_session_event_schema()), True)
    ])

def read_kafka_stream(spark, topic):
    """Read streaming data from a Kafka topic."""
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

def generate_uuid():
    """Generate UUID string for missing IDs."""
    return str(uuid.uuid4())

# Register UDF for UUID generation
generate_uuid_udf = udf(generate_uuid, StringType())

def process_users_data(df):
    """
    Process user data with validation rules.
    
    Rules:
    - If email is NULL → reject (required for contact)
    
    Returns:
        tuple: (valid_df, invalid_df)
    """
    processed_df = df.withColumn(
        "processing_valid",
        when(col("email").isNull(), lit(False)).otherwise(lit(True))
    ).withColumn(
        "processing_rejection_reason",
        when(col("email").isNull(), lit("email is required for user contact")).otherwise(lit(None))
    )
    
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)

def process_products_data(df):
    """
    Process product data with validation and transformation rules.
    
    Rules:
    - If product_id is NULL → generate UUID
    - If price < 0 → reject
    - If inventory < 0 → reject
    - If ratings < 0 → set to 0
    """
    processed_df = df.withColumn(
        "product_id",
        when(col("product_id").isNull(), generate_uuid_udf()).otherwise(col("product_id"))
    ).withColumn(
        "ratings",
        when(col("ratings") < 0, lit(0.0)).otherwise(col("ratings"))
    ).withColumn(
        "processing_valid",
        when(
            (col("price") < 0) | (col("inventory") < 0),
            lit(False)
        ).otherwise(lit(True))
    ).withColumn(
        "processing_rejection_reason",
        when(col("price") < 0, lit("negative price"))
        .when(col("inventory") < 0, lit("negative inventory"))
        .otherwise(lit(None))
    )
    
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)

def process_transactions_data(df):
    """
    Process transaction data with validation rules.
    
    Rules:
    - If payment_method is NULL → reject
    """
    processed_df = df.withColumn(
        "processing_valid",
        when(col("payment_method").isNull(), lit(False)).otherwise(lit(True))
    ).withColumn(
        "processing_rejection_reason",
        when(col("payment_method").isNull(), lit("payment_method is required")).otherwise(lit(None))
    )
    
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)

def process_sessions_data(df):
    """
    Process session data with ID generation.
    
    Rules:
    - If session_id is NULL → generate UUID
    """
    processed_df = df.withColumn(
        "session_id",
        when(col("session_id").isNull(), generate_uuid_udf()).otherwise(col("session_id"))
    )
    
    invalid_df = processed_df.filter(lit(False)).withColumn("processing_rejection_reason", lit(None))
    
    return (processed_df, invalid_df)

def write_invalid_to_console(df, topic_name, rejection_reason_col="rejection_reason"):
    """Write invalid records to console for debugging/audit."""
    query = df \
        .withColumn("audit_timestamp", current_timestamp()) \
        .withColumn("source_topic", lit(topic_name)) \
        .writeStream \
        .format("console") \
        .option("truncate", "false") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic_name}_audit") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Started streaming invalid {topic_name} records to console")
    return query

def write_processed_to_kafka(df, processed_topic, checkpoint_suffix):
    """Write processed data to Kafka topic for downstream consumers."""
    kafka_df = df.select(
        to_json(struct(*df.columns)).alias("value")
    )
    
    query = kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("topic", processed_topic) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{checkpoint_suffix}_kafka") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Started streaming processed data to Kafka topic: {processed_topic}")
    return query


# ==================== MongoDB Write Functions (using PyMongo) ====================

def get_mongodb_client():
    """Get a MongoDB client connection."""
    return MongoClient(MONGODB_URI)


def generate_time_id_from_str(timestamp_str):
    """Generate time_id in format YYYYMMDDHH from timestamp string."""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.strftime('%Y%m%d%H')
    except:
        dt = datetime.now()
        return dt.strftime('%Y%m%d%H')


def write_time_dimension(client, timestamp_str):
    """Write time dimension to MongoDB if not exists."""
    time_id = generate_time_id_from_str(timestamp_str)
    
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except:
        dt = datetime.now()
    
    db = client[MONGODB_DATABASE]
    collection = db["dim_time"]
    
    # Upsert time dimension
    collection.update_one(
        {"time_id": time_id},
        {"$setOnInsert": {
            "time_id": time_id,
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "day_of_week": dt.weekday(),
            "quarter": (dt.month - 1) // 3 + 1,
            "is_weekend": dt.weekday() >= 5
        }},
        upsert=True
    )
    
    return time_id


def write_users_to_mongodb(batch_df, batch_id):
    """Write user batch to MongoDB dim_users collection using PyMongo."""
    if batch_df.isEmpty():
        return
    
    count = batch_df.count()
    print(f"Writing users batch {batch_id} with {count} records to MongoDB...")
    
    try:
        client = get_mongodb_client()
        db = client[MONGODB_DATABASE]
        collection = db["dim_users"]
        
        now = datetime.now()
        documents = []
        
        for row in batch_df.collect():
            doc = {
                "user_id": row.user_id,
                "email": row.email,
                "age": row.age,
                "country": row.country,
                "registration_date": row.registeration_date,
                "created_at": now,
                "updated_at": now
            }
            documents.append(doc)
        
        if documents:
            collection.insert_many(documents, ordered=False)
        
        client.close()
        print(f"✓ Batch {batch_id}: Written {count} users to MongoDB")
    except BulkWriteError as e:
        print(f"✓ Batch {batch_id}: Written {count - len(e.details.get('writeErrors', []))} users (some duplicates skipped)")
    except Exception as e:
        print(f"✗ Error writing users batch {batch_id}: {e}")


def write_products_to_mongodb(batch_df, batch_id):
    """Write product batch to MongoDB dim_products collection using PyMongo."""
    if batch_df.isEmpty():
        return
    
    count = batch_df.count()
    print(f"Writing products batch {batch_id} with {count} records to MongoDB...")
    
    try:
        client = get_mongodb_client()
        db = client[MONGODB_DATABASE]
        collection = db["dim_products"]
        
        now = datetime.now()
        documents = []
        
        for row in batch_df.collect():
            doc = {
                "product_id": row.product_id,
                "name": row.name,
                "category": row.category,
                "price": float(row.price) if row.price else None,
                "inventory": row.inventory,
                "rating": float(row.ratings) if row.ratings else None,
                "created_at": now,
                "updated_at": now
            }
            documents.append(doc)
        
        if documents:
            collection.insert_many(documents, ordered=False)
        
        client.close()
        print(f"✓ Batch {batch_id}: Written {count} products to MongoDB")
    except BulkWriteError as e:
        print(f"✓ Batch {batch_id}: Written {count - len(e.details.get('writeErrors', []))} products (some duplicates skipped)")
    except Exception as e:
        print(f"✗ Error writing products batch {batch_id}: {e}")


def write_transactions_to_mongodb(batch_df, batch_id):
    """Write transaction batch to MongoDB fact_transactions collection using PyMongo."""
    if batch_df.isEmpty():
        return
    
    count = batch_df.count()
    print(f"Writing transactions batch {batch_id} with {count} records to MongoDB...")
    
    try:
        client = get_mongodb_client()
        db = client[MONGODB_DATABASE]
        collection = db["fact_transactions"]
        
        documents = []
        
        for row in batch_df.collect():
            # Calculate total amount
            total_amount = 0.0
            products_list = []
            
            if row.products:
                for p in row.products:
                    price = float(p.price) if p.price else 0.0
                    qty = int(p.quantity) if p.quantity else 0
                    total_amount += price * qty
                    products_list.append({
                        "product_id": p.product_id,
                        "quantity": qty,
                        "price": price
                    })
            
            # Write time dimension
            time_id = write_time_dimension(client, row.timestamp)
            
            doc = {
                "transaction_id": row.transaction_id,
                "user_id": row.user_id,
                "time_id": time_id,
                "timestamp": row.timestamp,
                "total_amount": total_amount,
                "payment_method": row.payment_method,
                "products": products_list,
                "num_products": len(products_list)
            }
            documents.append(doc)
        
        if documents:
            collection.insert_many(documents, ordered=False)
        
        client.close()
        print(f"✓ Batch {batch_id}: Written {count} transactions to MongoDB")
    except BulkWriteError as e:
        print(f"✓ Batch {batch_id}: Written {count - len(e.details.get('writeErrors', []))} transactions (some duplicates skipped)")
    except Exception as e:
        print(f"✗ Error writing transactions batch {batch_id}: {e}")


def write_sessions_to_mongodb(batch_df, batch_id):
    """Write session batch to MongoDB fact_sessions collection using PyMongo."""
    if batch_df.isEmpty():
        return
    
    count = batch_df.count()
    print(f"Writing sessions batch {batch_id} with {count} records to MongoDB...")
    
    try:
        client = get_mongodb_client()
        db = client[MONGODB_DATABASE]
        collection = db["fact_sessions"]
        
        documents = []
        
        for row in batch_df.collect():
            events_list = []
            
            if row.events:
                for e in row.events:
                    events_list.append({
                        "event_type": e.eventType,
                        "event_timestamp": e.timestamp
                    })
            
            # Write time dimension
            time_id = write_time_dimension(client, row.timestamp)
            
            doc = {
                "session_id": row.session_id,
                "user_id": row.user_id,
                "time_id": time_id,
                "timestamp": row.timestamp,
                "num_events": len(events_list),
                "session_duration": len(events_list) * 10,  # Estimate 10 sec per event
                "events": events_list
            }
            documents.append(doc)
        
        if documents:
            collection.insert_many(documents, ordered=False)
        
        client.close()
        print(f"✓ Batch {batch_id}: Written {count} sessions to MongoDB")
    except BulkWriteError as e:
        print(f"✓ Batch {batch_id}: Written {count - len(e.details.get('writeErrors', []))} sessions (some duplicates skipped)")
    except Exception as e:
        print(f"✗ Error writing sessions batch {batch_id}: {e}")


# ==================== Stream Processing Functions ====================

def process_users(spark):
    """Process user stream with three-way split."""
    topic = TOPICS['users']
    processed_topic = PROCESSED_TOPICS['users']
    
    raw_stream = read_kafka_stream(spark, topic)
    
    user_schema = get_user_schema()
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), user_schema).alias("data")) \
        .select("data.*")
    
    validation_stream = parsed_stream.withColumn(
        "is_valid",
        when(col("user_id").isNotNull(), lit(True)).otherwise(lit(False))
    ).withColumn(
        "rejection_reason",
        when(col("user_id").isNull(), lit("user_id is null")).otherwise(lit(None))
    )
    
    validation_invalid_stream = validation_stream.filter(col("is_valid") == False)
    valid_stream = validation_stream.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    
    processed_valid_stream, processing_invalid_stream = process_users_data(valid_stream)
    
    processing_invalid_renamed = processing_invalid_stream.withColumnRenamed("processing_rejection_reason", "rejection_reason")
    combined_invalid_stream = validation_invalid_stream.unionByName(processing_invalid_renamed, allowMissingColumns=True)
    
    audit_query = write_invalid_to_console(combined_invalid_stream, topic)
    kafka_query = write_processed_to_kafka(processed_valid_stream, processed_topic, f"{topic}_processed")
    
    mongodb_query = processed_valid_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_users_to_mongodb(batch_df, batch_id)) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_mongodb") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Three-stream processing started for {topic}")
    return [audit_query, kafka_query, mongodb_query]


def process_products(spark):
    """Process product stream with three-way split."""
    topic = TOPICS['products']
    processed_topic = PROCESSED_TOPICS['products']
    
    raw_stream = read_kafka_stream(spark, topic)
    
    product_schema = get_product_schema()
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), product_schema).alias("data")) \
        .select("data.*")
    
    validation_stream = parsed_stream.withColumn(
        "is_valid",
        when(col("name").isNotNull(), lit(True)).otherwise(lit(False))
    ).withColumn(
        "rejection_reason",
        when(col("name").isNull(), lit("name is null")).otherwise(lit(None))
    )
    
    validation_invalid_stream = validation_stream.filter(col("is_valid") == False)
    valid_stream = validation_stream.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    
    processed_valid_stream, processing_invalid_stream = process_products_data(valid_stream)
    
    processing_invalid_renamed = processing_invalid_stream.withColumnRenamed("processing_rejection_reason", "rejection_reason")
    combined_invalid_stream = validation_invalid_stream.unionByName(processing_invalid_renamed, allowMissingColumns=True)
    
    audit_query = write_invalid_to_console(combined_invalid_stream, topic)
    kafka_query = write_processed_to_kafka(processed_valid_stream, processed_topic, f"{topic}_processed")
    
    mongodb_query = processed_valid_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_products_to_mongodb(batch_df, batch_id)) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_mongodb") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Three-stream processing started for {topic}")
    return [audit_query, kafka_query, mongodb_query]


def process_transactions(spark):
    """Process transaction stream with three-way split."""
    topic = TOPICS['transactions']
    processed_topic = PROCESSED_TOPICS['transactions']
    
    raw_stream = read_kafka_stream(spark, topic)
    
    transaction_schema = get_transaction_schema()
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), transaction_schema).alias("data")) \
        .select("data.*")
    
    validation_stream = parsed_stream.withColumn(
        "is_valid",
        when(
            col("transaction_id").isNotNull() & 
            col("user_id").isNotNull() &
            col("products").isNotNull() &
            (size(col("products")) > 0),
            lit(True)
        ).otherwise(lit(False))
    ).withColumn(
        "rejection_reason",
        when(col("transaction_id").isNull(), lit("transaction_id is null"))
        .when(col("user_id").isNull(), lit("user_id is null"))
        .when(col("products").isNull(), lit("products is null"))
        .when(size(col("products")) == 0, lit("products array is empty"))
        .otherwise(lit(None))
    )
    
    validation_invalid_stream = validation_stream.filter(col("is_valid") == False)
    valid_stream = validation_stream.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    
    processed_valid_stream, processing_invalid_stream = process_transactions_data(valid_stream)
    
    processing_invalid_renamed = processing_invalid_stream.withColumnRenamed("processing_rejection_reason", "rejection_reason")
    combined_invalid_stream = validation_invalid_stream.unionByName(processing_invalid_renamed, allowMissingColumns=True)
    
    audit_query = write_invalid_to_console(combined_invalid_stream, topic)
    kafka_query = write_processed_to_kafka(processed_valid_stream, processed_topic, f"{topic}_processed")
    
    mongodb_query = processed_valid_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_transactions_to_mongodb(batch_df, batch_id)) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_mongodb") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Three-stream processing started for {topic}")
    return [audit_query, kafka_query, mongodb_query]


def process_sessions(spark):
    """Process session stream with three-way split."""
    topic = TOPICS['sessions']
    processed_topic = PROCESSED_TOPICS['sessions']
    
    raw_stream = read_kafka_stream(spark, topic)
    
    session_schema = get_session_schema()
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), session_schema).alias("data")) \
        .select("data.*")
    
    validation_stream = parsed_stream.withColumn(
        "is_valid",
        when(
            col("user_id").isNotNull() &
            col("events").isNotNull() &
            (size(col("events")) > 0),
            lit(True)
        ).otherwise(lit(False))
    ).withColumn(
        "rejection_reason",
        when(col("user_id").isNull(), lit("user_id is null"))
        .when(col("events").isNull(), lit("events is null"))
        .when(size(col("events")) == 0, lit("events array is empty"))
        .otherwise(lit(None))
    )
    
    validation_invalid_stream = validation_stream.filter(col("is_valid") == False)
    valid_stream = validation_stream.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    
    processed_valid_stream, processing_invalid_stream = process_sessions_data(valid_stream)
    
    processing_invalid_renamed = processing_invalid_stream.withColumnRenamed("processing_rejection_reason", "rejection_reason")
    combined_invalid_stream = validation_invalid_stream.unionByName(processing_invalid_renamed, allowMissingColumns=True)
    
    audit_query = write_invalid_to_console(combined_invalid_stream, topic)
    kafka_query = write_processed_to_kafka(processed_valid_stream, processed_topic, f"{topic}_processed")
    
    mongodb_query = processed_valid_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_sessions_to_mongodb(batch_df, batch_id)) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_mongodb") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Three-stream processing started for {topic}")
    return [audit_query, kafka_query, mongodb_query]


def main():
    """Main execution function."""
    print("=" * 70)
    print("GlobalMart Spark Streaming ELT/ETL Pipeline (Three-Stream Architecture)")
    print("=" * 70)
    print(f"Start time: {datetime.now().isoformat()}")
    print(f"\nConfiguration:")
    print(f"  Kafka Brokers: {KAFKA_BROKERS}")
    print(f"  MongoDB URI: {MONGODB_URI}")
    print(f"  MongoDB Database: {MONGODB_DATABASE}")
    print(f"  Checkpoint Path: {CHECKPOINT_PATH}")
    print(f"\nSource Topics:")
    for key, topic in TOPICS.items():
        print(f"  {key}: {topic}")
    print(f"\nProcessed Topics (Output):")
    for key, topic in PROCESSED_TOPICS.items():
        print(f"  {key}: {topic}")
    print("=" * 70)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Start all streaming queries
    print("\n=== Starting Streaming Pipelines ===")
    
    queries = []
    
    # Process each stream
    queries.extend(process_users(spark))
    queries.extend(process_products(spark))
    queries.extend(process_transactions(spark))
    queries.extend(process_sessions(spark))
    
    print(f"\n✓ All {len(queries)} streaming queries started successfully!")
    print("\nMonitoring:")
    print("  - Spark UI: http://localhost:4041")
    print("  - MongoDB: mongodb://localhost:27017")
    print("\nPress Ctrl+C to stop the pipeline...")
    
    # Wait for termination
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\n\nStopping streaming queries...")
        for query in queries:
            query.stop()
        print("✓ All queries stopped")
        spark.stop()
        print("✓ Spark session closed")


if __name__ == "__main__":
    main()
