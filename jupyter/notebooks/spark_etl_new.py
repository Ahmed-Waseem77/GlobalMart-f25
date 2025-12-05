#!/usr/bin/env python3
"""
Spark Streaming ELT Pipeline for GlobalMart

This pipeline implements a three-stream data flow:
1. Extract: Read streaming data from Kafka topics
2. Transform: Parse JSON and validate data using Spark DataFrame operations
3. Load (Three streams):
   a. Invalid/Anomalous data -> MongoDB 'audit_anomalies' collection
   b. Valid data -> Process (stub) -> Kafka processed topics
   c. Valid data -> Process (stub) -> MongoDB star schema (via PyMongo)

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_elt_flow.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp, 
    date_format, year, month, dayofmonth, hour, dayofweek, quarter,
    size, when, lit, concat_ws, concat, lpad, udf, expr, to_date
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

# Local checkpoint path
CHECKPOINT_PATH = "/tmp/spark_checkpoints"


def create_spark_session():
    """Create and configure Spark session."""
    
    # -----------------------------------------------------------------------
    # ⚠️ STEP 1: CHANGE THIS TO MATCH THE OUTPUT OF 'spark-submit --version'
    # Common versions: "3.5.1", "3.5.0", "3.4.1"
    # -----------------------------------------------------------------------
    SPARK_VERSION = "3.5.1"  
    SCALA_VERSION = "2.12"   
    
    # Define the package coordinates based on the version
    KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"
    
    print(f"--- Spark Configuration ---")
    print(f"Target Spark Version: {SPARK_VERSION}")
    print(f"Using Kafka Package:  {KAFKA_PACKAGE}")
    print(f"---------------------------")
    
    spark = SparkSession.builder \
        .appName("GlobalMart-ELT-Pipeline") \
        .config("spark.jars.packages", KAFKA_PACKAGE) \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"✓ Spark session created successfully")
    return spark


# --- Schemas ---

def get_user_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("registeration_date", StringType(), True) 
    ])

def get_product_schema():
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", FloatType(), True),
        StructField("inventory", IntegerType(), True),
        StructField("ratings", FloatType(), True)
    ])

def get_transaction_product_schema():
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", FloatType(), True)
    ])

def get_transaction_schema():
    return StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("products", ArrayType(get_transaction_product_schema()), True),
        StructField("payment_method", StringType(), True)
    ])

def get_session_event_schema():
    return StructType([
        StructField("eventType", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])

def get_session_schema():
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("events", ArrayType(get_session_event_schema()), True)
    ])

# --- Helper Functions ---

def read_kafka_stream(spark, topic):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()

def generate_uuid():
    return str(uuid.uuid4())

generate_uuid_udf = udf(generate_uuid, StringType())

def get_mongodb_client():
    return MongoClient(MONGODB_URI)

def generate_time_id_from_str(timestamp_str):
    """Generate time_id in format YYYYMMDDHH from timestamp string."""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.strftime('%Y%m%d%H')
    except:
        dt = datetime.now()
        return dt.strftime('%Y%m%d%H')

def upsert_time_dimension(db, timestamp_str):
    """Helper to upsert time dimension within a partition."""
    time_id = generate_time_id_from_str(timestamp_str)
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except:
        dt = datetime.now()
    
    # We use update_one with upsert=True to avoid duplicates
    db["dim_time"].update_one(
        {"time_id": time_id},
        {"$setOnInsert": {
            "time_id": time_id, "year": dt.year, "month": dt.month,
            "day": dt.day, "hour": dt.hour, "day_of_week": dt.weekday(),
            "quarter": (dt.month - 1) // 3 + 1, "is_weekend": dt.weekday() >= 5
        }},
        upsert=True
    )
    return time_id


# --- Validation Logic (Anomalies & Integrity) ---

def process_users_data(df):
    processed_df = df.withColumn(
        "processing_valid",
        when(col("email").isNull(), lit(False))
        .when(col("registeration_date") > current_timestamp(), lit(False)) # Future Date Anomaly
        .otherwise(lit(True))
    ).withColumn(
        "processing_rejection_reason",
        when(col("email").isNull(), lit("email is required"))
        .when(col("registeration_date") > current_timestamp(), lit("Time Travel: Registration date in future"))
        .otherwise(lit(None))
    )
    
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)

def process_products_data(df):
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
        when(col("price") < 0, lit("Data Quality: Negative price detected"))
        .when(col("inventory") < 0, lit("Data Quality: Negative inventory detected"))
        .otherwise(lit(None))
    )
    
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)

def process_transactions_data(df):
    # Check if any product in the array has quantity > 50
    has_bulk_items = expr("exists(products, x -> x.quantity > 50)")
    
    processed_df = df.withColumn(
        "processing_valid",
        when(col("payment_method") == "Unknown_Method", lit(False))
        .when(has_bulk_items, lit(False))
        .when(col("payment_method").isNull(), lit(False))
        .otherwise(lit(True))
    ).withColumn(
        "processing_rejection_reason",
        when(col("payment_method").isNull(), lit("payment_method is required"))
        .when(col("payment_method") == "Unknown_Method", lit("Fraud Check: Invalid Payment Method"))
        .when(has_bulk_items, lit("Abnormal Behavior: Bulk buying detected (Qty > 50)"))
        .otherwise(lit(None))
    )
    
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)

def process_sessions_data(df):
    processed_df = df.withColumn(
        "session_id",
        when(col("session_id").isNull(), generate_uuid_udf()).otherwise(col("session_id"))
    ).withColumn(
        "processing_valid",
        when(size(col("events")) > 50, lit(False)).otherwise(lit(True))
    ).withColumn(
        "processing_rejection_reason",
        when(size(col("events")) > 50, lit("Bot Detection: High velocity events (>50)"))
        .otherwise(lit(None))
    )
    
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)


# ==================== Partition Writers (Worker Side) ====================

def write_users_partition(iterator):
    client = get_mongodb_client()
    db = client[MONGODB_DATABASE]
    collection = db["dim_users"]
    batch = []
    now = datetime.now()
    try:
        for row in iterator:
            batch.append({
                "user_id": row.user_id, "email": row.email, "age": row.age,
                "country": row.country, "registration_date": row.registeration_date,
                "created_at": now, "updated_at": now
            })
            if len(batch) >= 1000:
                collection.insert_many(batch, ordered=False)
                batch = []
        if batch: collection.insert_many(batch, ordered=False)
    except BulkWriteError: pass
    except Exception as e: print(f"User Write Error: {e}")
    finally: client.close()

def write_products_partition(iterator):
    client = get_mongodb_client()
    db = client[MONGODB_DATABASE]
    collection = db["dim_products"]
    batch = []
    now = datetime.now()
    try:
        for row in iterator:
            batch.append({
                "product_id": row.product_id, "name": row.name, "category": row.category,
                "price": float(row.price) if row.price else None,
                "inventory": row.inventory, 
                "rating": float(row.ratings) if row.ratings else None,
                "created_at": now, "updated_at": now
            })
            if len(batch) >= 1000:
                collection.insert_many(batch, ordered=False)
                batch = []
        if batch: collection.insert_many(batch, ordered=False)
    except BulkWriteError: pass
    except Exception as e: print(f"Product Write Error: {e}")
    finally: client.close()

def write_transactions_partition(iterator):
    client = get_mongodb_client()
    db = client[MONGODB_DATABASE]
    collection = db["fact_transactions"]
    batch = []
    try:
        for row in iterator:
            total_amount = sum([(p.price or 0) * (p.quantity or 0) for p in (row.products or [])])
            products_list = [{"product_id": p.product_id, "quantity": p.quantity, "price": p.price} for p in (row.products or [])]
            time_id = upsert_time_dimension(db, row.timestamp)
            
            batch.append({
                "transaction_id": row.transaction_id, "user_id": row.user_id,
                "time_id": time_id, "timestamp": row.timestamp,
                "total_amount": total_amount, "payment_method": row.payment_method,
                "products": products_list, "num_products": len(products_list)
            })
            if len(batch) >= 1000:
                collection.insert_many(batch, ordered=False)
                batch = []
        if batch: collection.insert_many(batch, ordered=False)
    except BulkWriteError: pass
    except Exception as e: print(f"Transaction Write Error: {e}")
    finally: client.close()

def write_sessions_partition(iterator):
    client = get_mongodb_client()
    db = client[MONGODB_DATABASE]
    collection = db["fact_sessions"]
    batch = []
    try:
        for row in iterator:
            events_list = [{"event_type": e.eventType, "event_timestamp": e.timestamp} for e in (row.events or [])]
            time_id = upsert_time_dimension(db, row.timestamp)
            
            batch.append({
                "session_id": row.session_id, "user_id": row.user_id,
                "time_id": time_id, "timestamp": row.timestamp,
                "num_events": len(events_list),
                "session_duration": len(events_list) * 10,
                "events": events_list
            })
            if len(batch) >= 1000:
                collection.insert_many(batch, ordered=False)
                batch = []
        if batch: collection.insert_many(batch, ordered=False)
    except BulkWriteError: pass
    except Exception as e: print(f"Session Write Error: {e}")
    finally: client.close()

def write_audit_partition(iterator, source_collection):
    client = get_mongodb_client()
    db = client[MONGODB_DATABASE]
    collection = db["audit_anomalies"]
    batch = []
    try:
        for row in iterator:
            row_dict = row.asDict(recursive=True)
            reason = row_dict.pop('rejection_reason', 'Unknown')
            
            batch.append({
                "anomaly_id": str(uuid.uuid4()),
                "source_collection": source_collection,
                "anomaly_type": "Data Quality/Business Logic",
                "description": reason,
                "severity": "WARNING",
                "detected_at": datetime.now(),
                "raw_data": row_dict
            })
        if batch: collection.insert_many(batch, ordered=False)
    except Exception as e: print(f"Audit Write Error: {e}")
    finally: client.close()


# ==================== Write Entry Points (Driver Side) ====================

def write_users_to_mongodb(batch_df, batch_id):
    batch_df.rdd.foreachPartition(write_users_partition)
    print(f"✓ Batch {batch_id}: Processed Users")

def write_products_to_mongodb(batch_df, batch_id):
    batch_df.rdd.foreachPartition(write_products_partition)
    print(f"✓ Batch {batch_id}: Processed Products")

def write_transactions_to_mongodb(batch_df, batch_id):
    batch_df.rdd.foreachPartition(write_transactions_partition)
    print(f"✓ Batch {batch_id}: Processed Transactions")

def write_sessions_to_mongodb(batch_df, batch_id):
    batch_df.rdd.foreachPartition(write_sessions_partition)
    print(f"✓ Batch {batch_id}: Processed Sessions")

def write_audit_to_mongodb(batch_df, batch_id, source_collection):
    if not batch_df.isEmpty():
        batch_df.rdd.foreachPartition(lambda iter: write_audit_partition(iter, source_collection))
        print(f"⚠ Batch {batch_id}: Audited Anomalies for {source_collection}")

def write_processed_to_kafka(df, processed_topic, checkpoint_suffix):
    kafka_df = df.select(to_json(struct(*df.columns)).alias("value"))
    return kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("topic", processed_topic) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{checkpoint_suffix}_kafka") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()

# ==================== Stream Pipelines ====================

def process_users(spark):
    topic = TOPICS['users']
    parsed = read_kafka_stream(spark, topic).select(from_json(col("value").cast("string"), get_user_schema()).alias("data")).select("data.*")
    
    valid_stream, invalid_stream = process_users_data(parsed)
    
    audit_query = invalid_stream.writeStream \
        .foreachBatch(lambda df, id: write_audit_to_mongodb(df, id, "dim_users")) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_audit") \
        .start()
        
    kafka_query = write_processed_to_kafka(valid_stream, PROCESSED_TOPICS['users'], f"{topic}_processed")
    mongo_query = valid_stream.writeStream \
        .foreachBatch(write_users_to_mongodb) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_mongodb") \
        .start()
        
    return [audit_query, kafka_query, mongo_query]

def process_products(spark):
    topic = TOPICS['products']
    parsed = read_kafka_stream(spark, topic).select(from_json(col("value").cast("string"), get_product_schema()).alias("data")).select("data.*")
    
    valid_stream, invalid_stream = process_products_data(parsed)
    
    audit_query = invalid_stream.writeStream \
        .foreachBatch(lambda df, id: write_audit_to_mongodb(df, id, "dim_products")) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_audit") \
        .start()
        
    kafka_query = write_processed_to_kafka(valid_stream, PROCESSED_TOPICS['products'], f"{topic}_processed")
    mongo_query = valid_stream.writeStream \
        .foreachBatch(write_products_to_mongodb) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_mongodb") \
        .start()
        
    return [audit_query, kafka_query, mongo_query]

def process_transactions(spark):
    topic = TOPICS['transactions']
    parsed = read_kafka_stream(spark, topic).select(from_json(col("value").cast("string"), get_transaction_schema()).alias("data")).select("data.*")
    
    valid_stream, invalid_stream = process_transactions_data(parsed)
    
    audit_query = invalid_stream.writeStream \
        .foreachBatch(lambda df, id: write_audit_to_mongodb(df, id, "fact_transactions")) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_audit") \
        .start()
        
    kafka_query = write_processed_to_kafka(valid_stream, PROCESSED_TOPICS['transactions'], f"{topic}_processed")
    mongo_query = valid_stream.writeStream \
        .foreachBatch(write_transactions_to_mongodb) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_mongodb") \
        .start()
        
    return [audit_query, kafka_query, mongo_query]

def process_sessions(spark):
    topic = TOPICS['sessions']
    parsed = read_kafka_stream(spark, topic).select(from_json(col("value").cast("string"), get_session_schema()).alias("data")).select("data.*")
    
    valid_stream, invalid_stream = process_sessions_data(parsed)
    
    audit_query = invalid_stream.writeStream \
        .foreachBatch(lambda df, id: write_audit_to_mongodb(df, id, "fact_sessions")) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_audit") \
        .start()
        
    kafka_query = write_processed_to_kafka(valid_stream, PROCESSED_TOPICS['sessions'], f"{topic}_processed")
    mongo_query = valid_stream.writeStream \
        .foreachBatch(write_sessions_to_mongodb) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_mongodb") \
        .start()
        
    return [audit_query, kafka_query, mongo_query]

def main():
    print("=" * 60)
    print("GlobalMart Spark ELT: Anomaly Detection & Serialization Fixed")
    print("=" * 60)
    
    spark = create_spark_session()
    
    queries = []
    queries.extend(process_users(spark))
    queries.extend(process_products(spark))
    queries.extend(process_transactions(spark))
    queries.extend(process_sessions(spark))
    
    print(f"\n✓ Started {len(queries)} streams.")
    print("✓ Data flow: Kafka -> Spark -> (MongoDB / Audit / Kafka)")
    
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\nStopping...")
        spark.stop()

if __name__ == "__main__":
    main()