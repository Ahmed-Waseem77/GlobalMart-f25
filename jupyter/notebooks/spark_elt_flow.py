#!/usr/bin/env python3
"""
Spark Streaming ELT Pipeline for GlobalMart

This pipeline implements a three-stream data flow:
1. Extract: Read streaming data from Kafka topics
2. Transform: Parse JSON and validate data using Spark DataFrame operations
3. Load (Three streams):
   a. Invalid data -> HDFS audit path (/audit/<topic>/) for manual review
   b. Valid data -> Process (stub) -> Kafka processed topics
   c. Valid data -> Process (stub) -> HBase star schema

All processing is done using Spark DataFrame operations on the cluster.

Topics consumed:
- new_users
- new_transactions
- new_products
- new_sessions

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 spark_elt_flow.py
    Or run directly in Jupyter with PySpark kernel
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp, 
    date_format, year, month, dayofmonth, hour, dayofweek, quarter,
    size, when, lit, concat_ws, concat, lpad, udf
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, ArrayType, TimestampType
)
import happybase
import uuid
from datetime import datetime

# Configuration
KAFKA_BROKERS = "kafka1:9092,kafka2:9092"
HDFS_NAMENODE = "hdfs://namenode:8020"
HBASE_HOST = "hbase-thrift"
HBASE_PORT = 9091

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

# HDFS paths
HDFS_AUDIT_PATH = f"{HDFS_NAMENODE}/audit"
CHECKPOINT_PATH = f"{HDFS_NAMENODE}/checkpoints"

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
        .option("startingOffsets", "latest") \
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
    # Apply processing logic using Spark DataFrame operations
    processed_df = df.withColumn(
        "processing_valid",
        when(col("email").isNull(), lit(False)).otherwise(lit(True))
    ).withColumn(
        "processing_rejection_reason",
        when(col("email").isNull(), lit("email is required for user contact")).otherwise(lit(None))
    )
    
    # Split into valid and invalid
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
    - NULL category is acceptable
    
    Returns:
        tuple: (valid_df, invalid_df)
    """
    # Apply transformations using Spark DataFrame operations
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
    
    # Split into valid and invalid
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)

def process_transactions_data(df):
    """
    Process transaction data with validation rules.
    
    Rules:
    - If payment_method is NULL → reject
    
    Returns:
        tuple: (valid_df, invalid_df)
    """
    # Apply processing logic using Spark DataFrame operations
    processed_df = df.withColumn(
        "processing_valid",
        when(col("payment_method").isNull(), lit(False)).otherwise(lit(True))
    ).withColumn(
        "processing_rejection_reason",
        when(col("payment_method").isNull(), lit("payment_method is required")).otherwise(lit(None))
    )
    
    # Split into valid and invalid
    valid_df = processed_df.filter(col("processing_valid") == True).drop("processing_valid", "processing_rejection_reason")
    invalid_df = processed_df.filter(col("processing_valid") == False).drop("processing_valid")
    
    return (valid_df, invalid_df)

def process_sessions_data(df):
    """
    Process session data with ID generation.
    
    Rules:
    - If session_id is NULL → generate UUID
    
    Returns:
        tuple: (valid_df, invalid_df) - invalid_df will be empty for sessions
    """
    # Apply transformations using Spark DataFrame operations
    processed_df = df.withColumn(
        "session_id",
        when(col("session_id").isNull(), generate_uuid_udf()).otherwise(col("session_id"))
    )
    
    # For sessions, all records are valid after ID generation
    # Return empty invalid DataFrame with same schema
    invalid_df = processed_df.filter(lit(False)).withColumn("processing_rejection_reason", lit(None))
    
    return (processed_df, invalid_df)

def write_invalid_to_hdfs(df, topic_name, rejection_reason_col="rejection_reason"):
    """
    Write invalid records to HDFS audit path for manual review.
    Uses Spark DataFrame operations for distributed processing.
    
    Args:
        df: Spark DataFrame with invalid records
        topic_name: Source topic name for partitioning
        rejection_reason_col: Column name containing rejection reason
        
    Returns:
        Streaming query handle
    """
    query = df \
        .withColumn("audit_date", date_format(current_timestamp(), "yyyy-MM-dd")) \
        .withColumn("audit_timestamp", current_timestamp()) \
        .writeStream \
        .format("parquet") \
        .option("path", f"{HDFS_AUDIT_PATH}/{topic_name}") \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic_name}_audit") \
        .partitionBy("audit_date") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Started streaming invalid {topic_name} records to HDFS audit")
    return query

def write_processed_to_kafka(df, processed_topic, checkpoint_suffix):
    """
    Write processed data to Kafka topic for downstream consumers.
    Uses Spark DataFrame operations for distributed processing.
    
    Args:
        df: Spark DataFrame with processed records
        processed_topic: Target Kafka topic name
        checkpoint_suffix: Unique checkpoint identifier
        
    Returns:
        Streaming query handle
    """
    # Convert DataFrame to JSON for Kafka
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

def process_users(spark):
    """
    Process user stream with three-way split:
    1. Invalid records -> HDFS audit (validation + processing rejections)
    2. Valid records -> Process -> Kafka
    3. Valid records -> Process -> HBase
    """
    topic = TOPICS['users']
    processed_topic = PROCESSED_TOPICS['users']
    
    # Read from Kafka
    raw_stream = read_kafka_stream(spark, topic)
    
    # Parse JSON using Spark operations
    user_schema = get_user_schema()
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), user_schema).alias("data")) \
        .select("data.*")
    
    # Initial validation using Spark DataFrame operations
    validation_stream = parsed_stream.withColumn(
        "is_valid",
        when(col("user_id").isNotNull(), lit(True)).otherwise(lit(False))
    ).withColumn(
        "rejection_reason",
        when(col("user_id").isNull(), lit("user_id is null")).otherwise(lit(None))
    )
    
    # Split: validation invalid records
    validation_invalid_stream = validation_stream.filter(col("is_valid") == False)
    
    # Valid records for processing
    valid_stream = validation_stream.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    
    # Apply processing (returns valid and invalid DataFrames)
    processed_valid_stream, processing_invalid_stream = process_users_data(valid_stream)
    
    # Merge validation and processing invalids
    # Rename processing_rejection_reason to rejection_reason for consistency
    processing_invalid_renamed = processing_invalid_stream.withColumnRenamed("processing_rejection_reason", "rejection_reason")
    combined_invalid_stream = validation_invalid_stream.unionByName(processing_invalid_renamed, allowMissingColumns=True)
    
    # Stream 1: Combined invalid records to HDFS audit
    audit_query = write_invalid_to_hdfs(combined_invalid_stream, topic)
    
    # Stream 2: Processed valid data to Kafka
    kafka_query = write_processed_to_kafka(processed_valid_stream, processed_topic, f"{topic}_processed")
    
    # Stream 3: Processed valid data to HBase
    hbase_query = processed_valid_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_users_to_hbase(batch_df, batch_id)) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_hbase") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Three-stream processing started for {topic}")
    return [audit_query, kafka_query, hbase_query]

def process_products(spark):
    """
    Process product stream with three-way split:
    1. Invalid records -> HDFS audit (validation + processing rejections)
    2. Valid records -> Process -> Kafka
    3. Valid records -> Process -> HBase
    """
    topic = TOPICS['products']
    processed_topic = PROCESSED_TOPICS['products']
    
    # Read from Kafka
    raw_stream = read_kafka_stream(spark, topic)
    
    # Parse JSON using Spark operations
    product_schema = get_product_schema()
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), product_schema).alias("data")) \
        .select("data.*")
    
    # Initial validation (only check name, product_id checked in processing)
    validation_stream = parsed_stream.withColumn(
        "is_valid",
        when(col("name").isNotNull(), lit(True)).otherwise(lit(False))
    ).withColumn(
        "rejection_reason",
        when(col("name").isNull(), lit("name is null")).otherwise(lit(None))
    )
    
    # Split: validation invalid records
    validation_invalid_stream = validation_stream.filter(col("is_valid") == False)
    
    # Valid records for processing
    valid_stream = validation_stream.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    
    # Apply processing (handles product_id generation, price/inventory/ratings validation)
    processed_valid_stream, processing_invalid_stream = process_products_data(valid_stream)
    
    # Merge validation and processing invalids
    processing_invalid_renamed = processing_invalid_stream.withColumnRenamed("processing_rejection_reason", "rejection_reason")
    combined_invalid_stream = validation_invalid_stream.unionByName(processing_invalid_renamed, allowMissingColumns=True)
    
    # Stream 1: Combined invalid records to HDFS audit
    audit_query = write_invalid_to_hdfs(combined_invalid_stream, topic)
    
    # Stream 2: Processed valid data to Kafka
    kafka_query = write_processed_to_kafka(processed_valid_stream, processed_topic, f"{topic}_processed")
    
    # Stream 3: Processed valid data to HBase
    hbase_query = processed_valid_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_products_to_hbase(batch_df, batch_id)) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_hbase") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Three-stream processing started for {topic}")
    return [audit_query, kafka_query, hbase_query]

def process_transactions(spark):
    """
    Process transaction stream with three-way split:
    1. Invalid records -> HDFS audit (validation + processing rejections)
    2. Valid records -> Process -> Kafka
    3. Valid records -> Process -> HBase
    """
    topic = TOPICS['transactions']
    processed_topic = PROCESSED_TOPICS['transactions']
    
    # Read from Kafka
    raw_stream = read_kafka_stream(spark, topic)
    
    # Parse JSON using Spark operations
    transaction_schema = get_transaction_schema()
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), transaction_schema).alias("data")) \
        .select("data.*")
    
    # Initial validation (payment_method checked in processing)
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
    
    # Split: validation invalid records
    validation_invalid_stream = validation_stream.filter(col("is_valid") == False)
    
    # Valid records for processing
    valid_stream = validation_stream.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    
    # Apply processing (handles payment_method validation)
    processed_valid_stream, processing_invalid_stream = process_transactions_data(valid_stream)
    
    # Merge validation and processing invalids
    processing_invalid_renamed = processing_invalid_stream.withColumnRenamed("processing_rejection_reason", "rejection_reason")
    combined_invalid_stream = validation_invalid_stream.unionByName(processing_invalid_renamed, allowMissingColumns=True)
    
    # Stream 1: Combined invalid records to HDFS audit
    audit_query = write_invalid_to_hdfs(combined_invalid_stream, topic)
    
    # Stream 2: Processed valid data to Kafka
    kafka_query = write_processed_to_kafka(processed_valid_stream, processed_topic, f"{topic}_processed")
    
    # Stream 3: Processed valid data to HBase
    hbase_query = processed_valid_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_transactions_to_hbase(batch_df, batch_id)) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_hbase") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Three-stream processing started for {topic}")
    return [audit_query, kafka_query, hbase_query]

def process_sessions(spark):
    """
    Process session stream with three-way split:
    1. Invalid records -> HDFS audit (validation rejections, processing generates IDs)
    2. Valid records -> Process -> Kafka
    3. Valid records -> Process -> HBase
    """
    topic = TOPICS['sessions']
    processed_topic = PROCESSED_TOPICS['sessions']
    
    # Read from Kafka
    raw_stream = read_kafka_stream(spark, topic)
    
    # Parse JSON using Spark operations
    session_schema = get_session_schema()
    parsed_stream = raw_stream \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .select(from_json(col("json_value"), session_schema).alias("data")) \
        .select("data.*")
    
    # Initial validation (session_id checked/generated in processing)
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
    
    # Split: validation invalid records
    validation_invalid_stream = validation_stream.filter(col("is_valid") == False)
    
    # Valid records for processing
    valid_stream = validation_stream.filter(col("is_valid") == True).drop("is_valid", "rejection_reason")
    
    # Apply processing (handles session_id generation)
    processed_valid_stream, processing_invalid_stream = process_sessions_data(valid_stream)
    
    # Merge validation and processing invalids (processing_invalid will be empty for sessions)
    processing_invalid_renamed = processing_invalid_stream.withColumnRenamed("processing_rejection_reason", "rejection_reason")
    combined_invalid_stream = validation_invalid_stream.unionByName(processing_invalid_renamed, allowMissingColumns=True)
    
    # Stream 1: Combined invalid records to HDFS audit
    audit_query = write_invalid_to_hdfs(combined_invalid_stream, topic)
    
    # Stream 2: Processed valid data to Kafka
    kafka_query = write_processed_to_kafka(processed_valid_stream, processed_topic, f"{topic}_processed")
    
    # Stream 3: Processed valid data to HBase
    hbase_query = processed_valid_stream.writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_sessions_to_hbase(batch_df, batch_id)) \
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/{topic}_hbase") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print(f"✓ Three-stream processing started for {topic}")
    return [audit_query, kafka_query, hbase_query]

# HBase write functions
def get_hbase_connection():
    """Get HBase connection."""
    return happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)

def write_users_to_hbase(batch_df, batch_id):
    """Write user batch to HBase dim_users table."""
    if batch_df.isEmpty():
        return
    
    print(f"Writing users batch {batch_id} with {batch_df.count()} records to HBase...")
    
    try:
        connection = get_hbase_connection()
        table = connection.table('dim_users')
        
        with table.batch(batch_size=1000) as batch:
            for row in batch_df.collect():
                if row.user_id:
                    batch.put(
                        row.user_id.encode(),
                        {
                            b'profile:email': str(row.email).encode() if row.email else b'',
                            b'profile:age': str(row.age).encode() if row.age else b'',
                            b'profile:country': str(row.country).encode() if row.country else b'',
                            b'profile:registration_date': str(row.registeration_date).encode() if row.registeration_date else b''
                        }
                    )
        
        connection.close()
        print(f"✓ Batch {batch_id}: Written {batch_df.count()} users to HBase")
    except Exception as e:
        print(f"✗ Error writing users batch {batch_id}: {e}")

def write_products_to_hbase(batch_df, batch_id):
    """Write product batch to HBase dim_products table."""
    if batch_df.isEmpty():
        return
    
    print(f"Writing products batch {batch_id} with {batch_df.count()} records to HBase...")
    
    try:
        connection = get_hbase_connection()
        table = connection.table('dim_products')
        
        with table.batch(batch_size=1000) as batch:
            for row in batch_df.collect():
                if row.product_id:
                    batch.put(
                        row.product_id.encode(),
                        {
                            b'details:name': str(row.name).encode() if row.name else b'',
                            b'details:category': str(row.category).encode() if row.category else b'',
                            b'details:price': str(row.price).encode() if row.price else b'',
                            b'details:inventory': str(row.inventory).encode() if row.inventory else b'',
                            b'details:ratings': str(row.ratings).encode() if row.ratings else b''
                        }
                    )
        
        connection.close()
        print(f"✓ Batch {batch_id}: Written {batch_df.count()} products to HBase")
    except Exception as e:
        print(f"✗ Error writing products batch {batch_id}: {e}")

def generate_time_id(timestamp_str):
    """Generate time_id in format YYYYMMDD_HH from timestamp string."""
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.strftime('%Y%m%d_%H')
    except:
        dt = datetime.now()
        return dt.strftime('%Y%m%d_%H')

def write_time_dimension(timestamp_str, connection):
    """Write to dim_time table if not exists."""
    time_id = generate_time_id(timestamp_str)
    
    try:
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except:
        dt = datetime.now()
    
    table = connection.table('dim_time')
    
    # Check if time_id already exists
    try:
        existing = table.row(time_id.encode())
        if existing:
            return time_id  # Already exists
    except:
        pass
    
    # Insert new time dimension
    table.put(
        time_id.encode(),
        {
            b'temporal:year': str(dt.year).encode(),
            b'temporal:month': str(dt.month).encode(),
            b'temporal:day': str(dt.day).encode(),
            b'temporal:hour': str(dt.hour).encode(),
            b'temporal:day_of_week': dt.strftime('%A').encode(),
            b'temporal:quarter': str((dt.month - 1) // 3 + 1).encode()
        }
    )
    
    return time_id

def write_transactions_to_hbase(batch_df, batch_id):
    """Write transaction batch to HBase fact_transactions table."""
    if batch_df.isEmpty():
        return
    
    print(f"Writing transactions batch {batch_id} with {batch_df.count()} records to HBase...")
    
    try:
        connection = get_hbase_connection()
        table = connection.table('fact_transactions')
        
        with table.batch(batch_size=1000) as batch:
            for row in batch_df.collect():
                if row.transaction_id and row.products:
                    # Calculate metrics
                    total_amount = sum(p.price * p.quantity for p in row.products if p.price and p.quantity)
                    num_products = len(row.products)
                    
                    # Generate time_id and populate dim_time
                    time_id = write_time_dimension(row.timestamp, connection)
                    
                    # Prepare HBase row
                    hbase_data = {
                        b'metrics:total_amount': str(total_amount).encode(),
                        b'metrics:num_products': str(num_products).encode(),
                        b'metrics:timestamp': str(row.timestamp).encode(),
                        b'refs:user_id': str(row.user_id).encode(),
                        b'refs:time_id': time_id.encode(),
                        b'refs:payment_method': str(row.payment_method).encode() if row.payment_method else b''
                    }
                    
                    # Add product details
                    for idx, product in enumerate(row.products):
                        if product.product_id:
                            hbase_data[f'products:product_{idx}_id'.encode()] = product.product_id.encode()
                            hbase_data[f'products:product_{idx}_quantity'.encode()] = str(product.quantity).encode() if product.quantity else b'0'
                            hbase_data[f'products:product_{idx}_price'.encode()] = str(product.price).encode() if product.price else b'0'
                    
                    batch.put(row.transaction_id.encode(), hbase_data)
        
        connection.close()
        print(f"✓ Batch {batch_id}: Written {batch_df.count()} transactions to HBase")
    except Exception as e:
        print(f"✗ Error writing transactions batch {batch_id}: {e}")

def write_sessions_to_hbase(batch_df, batch_id):
    """Write session batch to HBase fact_sessions table."""
    if batch_df.isEmpty():
        return
    
    print(f"Writing sessions batch {batch_id} with {batch_df.count()} records to HBase...")
    
    try:
        connection = get_hbase_connection()
        table = connection.table('fact_sessions')
        
        with table.batch(batch_size=1000) as batch:
            for row in batch_df.collect():
                if row.session_id and row.events:
                    # Calculate metrics
                    num_events = len(row.events)
                    
                    # Calculate session duration (simplified - using event count as proxy)
                    session_duration = num_events * 10  # Estimate 10 seconds per event
                    
                    # Generate time_id and populate dim_time
                    time_id = write_time_dimension(row.timestamp, connection)
                    
                    # Prepare HBase row
                    hbase_data = {
                        b'metrics:num_events': str(num_events).encode(),
                        b'metrics:session_duration': str(session_duration).encode(),
                        b'metrics:timestamp': str(row.timestamp).encode(),
                        b'refs:user_id': str(row.user_id).encode(),
                        b'refs:time_id': time_id.encode()
                    }
                    
                    # Add event details
                    for idx, event in enumerate(row.events):
                        if event.eventType:
                            hbase_data[f'events:event_{idx}_type'.encode()] = event.eventType.encode()
                            hbase_data[f'events:event_{idx}_timestamp'.encode()] = str(event.timestamp).encode() if event.timestamp else b''
                    
                    batch.put(row.session_id.encode(), hbase_data)
        
        connection.close()
        print(f"✓ Batch {batch_id}: Written {batch_df.count()} sessions to HBase")
    except Exception as e:
        print(f"✗ Error writing sessions batch {batch_id}: {e}")

def main():
    """Main execution function."""
    print("=" * 70)
    print("GlobalMart Spark Streaming ELT/ETL Pipeline (Three-Stream Architecture)")
    print("=" * 70)
    print(f"Start time: {datetime.now().isoformat()}")
    print(f"\nConfiguration:")
    print(f"  Kafka Brokers: {KAFKA_BROKERS}")
    print(f"  HDFS Namenode: {HDFS_NAMENODE}")
    print(f"  HDFS Audit Path: {HDFS_AUDIT_PATH}")
    print(f"  HBase Host: {HBASE_HOST}:{HBASE_PORT}")
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
    print("  - HDFS UI: http://localhost:9870")
    print("  - HBase UI: http://localhost:16010")
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
