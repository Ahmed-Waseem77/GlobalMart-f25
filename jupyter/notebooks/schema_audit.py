#!/usr/bin/env python3
"""
MongoDB Schema Creation Script for GlobalMart Star Schema

This script creates dimension, fact, and audit collections in MongoDB.
The schema follows a star pattern with:
- Dimension collections: dim_users, dim_products, dim_time
- Fact collections: fact_transactions, fact_sessions
- Audit collections: audit_anomalies (for storing data quality issues)

Usage:
    python create_mongodb_schema.py
"""

from pymongo import MongoClient
from pymongo.errors import CollectionInvalid, OperationFailure
import sys
from datetime import datetime

# MongoDB connection configuration
MONGODB_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
DATABASE_NAME = "globalmart"


def get_connection():
    """Establish connection to MongoDB replica set."""
    try:
        print(f"Connecting to MongoDB at {MONGODB_URI}...")
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("[SUCCESS] Connected to MongoDB successfully")
        
        try:
            rs_status = client.admin.command('replSetGetStatus')
            primary = next((m['name'] for m in rs_status['members'] if m['stateStr'] == 'PRIMARY'), 'Unknown')
            print(f"[INFO] Connected to replica set '{rs_status['set']}', primary: {primary}")
        except Exception:
            print("[INFO] Replica set status check skipped")
            
        return client
    except Exception as e:
        print(f"[ERROR] Failed to connect to MongoDB: {e}")
        sys.exit(1)


def create_collection_with_validation(db, collection_name, validator, indexes=None):
    """Create a collection with JSON Schema validation and optional indexes."""
    try:
        if collection_name in db.list_collection_names():
            print(f"[WARNING] Collection '{collection_name}' already exists, dropping...")
            db.drop_collection(collection_name)
        
        db.create_collection(collection_name, validator=validator)
        print(f"[SUCCESS] Created collection: {collection_name}")
        
        if indexes:
            collection = db[collection_name]
            for index in indexes:
                collection.create_index(index['keys'], **index.get('options', {}))
                print(f"  - Created index on {index['keys']}")
                
    except Exception as e:
        print(f"[ERROR] Error creating {collection_name}: {e}")


def create_dimension_collections(db):
    """Create dimension collections for the star schema."""
    print("\n=== Creating Dimension Collections ===")
    
    # Dimension: Users
    dim_users_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["user_id", "email"],
            "properties": {
                "user_id": {"bsonType": "string", "description": "Unique user identifier"},
                "email": {"bsonType": "string", "description": "User email address"},
                "age": {"bsonType": ["int", "null"], "minimum": 0, "maximum": 150},
                "country": {"bsonType": ["string", "null"]},
                "registration_date": {"bsonType": ["date", "string", "null"]},
                "created_at": {"bsonType": "date"},
                "updated_at": {"bsonType": "date"}
            }
        }
    }
    
    dim_users_indexes = [
        {"keys": [("user_id", 1)], "options": {"unique": True}},
        {"keys": [("email", 1)]},
        {"keys": [("country", 1)]}
    ]
    
    create_collection_with_validation(db, "dim_users", dim_users_validator, dim_users_indexes)
    
    # Dimension: Products
    dim_products_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["product_id", "name", "category"],
            "properties": {
                "product_id": {"bsonType": "string", "description": "Unique product identifier"},
                "name": {"bsonType": "string", "description": "Product name"},
                "category": {"bsonType": "string", "description": "Product category"},
                "price": {"bsonType": ["double", "int", "null"], "minimum": 0},
                "inventory": {"bsonType": ["int", "null"], "minimum": 0},
                "rating": {"bsonType": ["double", "int", "null"], "minimum": 0, "maximum": 5},
                "created_at": {"bsonType": "date"},
                "updated_at": {"bsonType": "date"}
            }
        }
    }
    
    dim_products_indexes = [
        {"keys": [("product_id", 1)], "options": {"unique": True}},
        {"keys": [("category", 1)]},
        {"keys": [("price", 1)]}
    ]
    
    create_collection_with_validation(db, "dim_products", dim_products_validator, dim_products_indexes)
    
    # Dimension: Time
    dim_time_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["time_id"],
            "properties": {
                "time_id": {"bsonType": "string", "description": "YYYYMMDDHH format"},
                "year": {"bsonType": "int"},
                "month": {"bsonType": "int", "minimum": 1, "maximum": 12},
                "day": {"bsonType": "int", "minimum": 1, "maximum": 31},
                "hour": {"bsonType": "int", "minimum": 0, "maximum": 23},
                "day_of_week": {"bsonType": "int", "minimum": 0, "maximum": 6},
                "quarter": {"bsonType": "int", "minimum": 1, "maximum": 4},
                "is_weekend": {"bsonType": "bool"}
            }
        }
    }
    
    dim_time_indexes = [
        {"keys": [("time_id", 1)], "options": {"unique": True}},
        {"keys": [("year", 1), ("month", 1), ("day", 1)]},
        {"keys": [("day_of_week", 1)]}
    ]
    
    create_collection_with_validation(db, "dim_time", dim_time_validator, dim_time_indexes)


def create_fact_collections(db):
    """Create fact collections for the star schema."""
    print("\n=== Creating Fact Collections ===")
    
    # Fact: Transactions
    fact_transactions_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["transaction_id", "user_id", "timestamp"],
            "properties": {
                "transaction_id": {"bsonType": "string"},
                "user_id": {"bsonType": "string"},
                "time_id": {"bsonType": "string"},
                "timestamp": {"bsonType": ["date", "string"]},
                "total_amount": {"bsonType": ["double", "int"]},
                "payment_method": {"bsonType": ["string", "null"]},
                "products": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "properties": {
                            "product_id": {"bsonType": "string"},
                            "quantity": {"bsonType": "int", "minimum": 1},
                            "price": {"bsonType": ["double", "int"]}
                        }
                    }
                },
                "num_products": {"bsonType": "int"}
            }
        }
    }
    
    fact_transactions_indexes = [
        {"keys": [("transaction_id", 1)], "options": {"unique": True}},
        {"keys": [("user_id", 1)]},
        {"keys": [("time_id", 1)]},
        {"keys": [("timestamp", -1)]},
        {"keys": [("payment_method", 1)]}
    ]
    
    create_collection_with_validation(db, "fact_transactions", fact_transactions_validator, fact_transactions_indexes)
    
    # Fact: Sessions
    fact_sessions_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["session_id", "user_id", "timestamp"],
            "properties": {
                "session_id": {"bsonType": "string"},
                "user_id": {"bsonType": "string"},
                "time_id": {"bsonType": "string"},
                "timestamp": {"bsonType": ["date", "string"]},
                "session_duration": {"bsonType": ["int", "double", "null"], "minimum": 0},
                "num_events": {"bsonType": "int"},
                "events": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "object",
                        "properties": {
                            "event_type": {"bsonType": "string"},
                            "event_timestamp": {"bsonType": ["date", "string"]}
                        }
                    }
                }
            }
        }
    }
    
    fact_sessions_indexes = [
        {"keys": [("session_id", 1)], "options": {"unique": True}},
        {"keys": [("user_id", 1)]},
        {"keys": [("time_id", 1)]},
        {"keys": [("timestamp", -1)]}
    ]
    
    create_collection_with_validation(db, "fact_sessions", fact_sessions_validator, fact_sessions_indexes)


def create_audit_collections(db):
    """Create audit collections for anomalies and data quality issues."""
    print("\n=== Creating Audit Collections ===")
    
    # Audit: Anomalies
    # This collection is more flexible to accommodate various types of failures
    audit_anomalies_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["anomaly_id", "anomaly_type", "detected_at", "raw_data"],
            "properties": {
                "anomaly_id": {
                    "bsonType": "string",
                    "description": "Unique identifier for the anomaly record"
                },
                "source_collection": {
                    "bsonType": "string",
                    "description": "Where this data was intended to go (e.g., fact_transactions)"
                },
                "anomaly_type": {
                    "bsonType": "string",
                    "description": "Type of error: 'Schema Validation', 'Negative Price', 'Future Date', 'Bot Activity'"
                },
                "description": {
                    "bsonType": "string",
                    "description": "Human readable description of why it failed"
                },
                "severity": {
                    "enum": ["CRITICAL", "WARNING", "INFO"],
                    "description": "Severity of the anomaly"
                },
                "detected_at": {
                    "bsonType": "date",
                    "description": "When the system caught this anomaly"
                },
                "raw_data": {
                    "bsonType": "object",
                    "description": "The actual full record that caused the anomaly (stored as-is)"
                }
            }
        }
    }
    
    audit_anomalies_indexes = [
        # Index on anomaly type for filtering reports
        {"keys": [("anomaly_type", 1)]},
        # Index on source to see which table has the most errors
        {"keys": [("source_collection", 1)]},
        # Index on time to query recent errors (or for TTL if you want auto-delete)
        {"keys": [("detected_at", -1)]} 
    ]
    
    create_collection_with_validation(db, "audit_anomalies", audit_anomalies_validator, audit_anomalies_indexes)


def verify_schema(db):
    """Verify all collections were created successfully."""
    print("\n=== Verifying Schema ===")
    
    expected_collections = [
        'dim_users',
        'dim_products',
        'dim_time',
        'fact_transactions',
        'fact_sessions',
        'audit_anomalies'
    ]
    
    actual_collections = db.list_collection_names()
    
    print(f"\nExpected collections: {', '.join(expected_collections)}")
    print(f"Found collections: {', '.join(actual_collections)}")
    
    missing_collections = [c for c in expected_collections if c not in actual_collections]
    if missing_collections:
        print(f"[ERROR] Missing collections: {', '.join(missing_collections)}")
        return False
    
    print("\n[SUCCESS] All collections created successfully!")
    
    # Show collection details
    print("\n=== Collection Details ===")
    for collection_name in expected_collections:
        try:
            collection = db[collection_name]
            indexes = list(collection.list_indexes())
            doc_count = collection.count_documents({})
            print(f"\n{collection_name}:")
            print(f"  - Documents: {doc_count}")
            print(f"  - Indexes: {len(indexes)}")
            for idx in indexes:
                print(f"    * {idx['name']}: {list(idx['key'].keys())}")
        except Exception as e:
            print(f"[ERROR] Error getting details for {collection_name}: {e}")
    
    return True


def main():
    """Main execution function."""
    print("=" * 60)
    print("MongoDB Star Schema & Audit Creation for GlobalMart")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    # Connect to MongoDB
    client = get_connection()
    
    try:
        # Get the database
        db = client[DATABASE_NAME]
        print(f"\n[INFO] Using database: {DATABASE_NAME}")
        
        # Create dimension collections
        create_dimension_collections(db)
        
        # Create fact collections
        create_fact_collections(db)
        
        # Create audit collections
        create_audit_collections(db)
        
        # Verify schema
        success = verify_schema(db)
        
        if success:
            print("\n" + "=" * 60)
            print("[SUCCESS] Schema creation completed successfully!")
            print("=" * 60)
            print("\nNext steps:")
            print("1. Start the Kafka producer")
            print("2. Run the Spark Pipeline (ensure it routes bad data to 'audit_anomalies')")
            print(f"\nMongoDB connection string: {MONGODB_URI}")
            print(f"Database: {DATABASE_NAME}")
        else:
            print("\n" + "=" * 60)
            print("[ERROR] Schema creation completed with errors")
            print("=" * 60)
            
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        raise
    finally:
        client.close()
        print("\nMongoDB connection closed.")


if __name__ == "__main__":
    main()