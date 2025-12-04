#!/usr/bin/env python3
"""
MongoDB Data Validation Script for GlobalMart Star Schema

This script connects to MongoDB and prints a sample of documents
from each collection in the star schema to verify data was loaded correctly.

Usage:
    python validate_mongodb_processed.py
"""

from pymongo import MongoClient
from datetime import datetime
import json

# MongoDB connection configuration
MONGODB_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
DATABASE_NAME = "globalmart"

# Collections to validate
COLLECTIONS = [
    # Dimension tables
    "dim_users",
    "dim_products", 
    "dim_time",
    # Fact tables
    "fact_transactions",
    "fact_sessions"
]

SAMPLE_SIZE = 3  # Number of documents to show per collection


def format_document(doc):
    """Format a MongoDB document for pretty printing."""
    # Convert ObjectId and datetime to strings for JSON serialization
    def convert(obj):
        if hasattr(obj, 'isoformat'):
            return obj.isoformat()
        elif hasattr(obj, '__str__') and type(obj).__name__ == 'ObjectId':
            return str(obj)
        return obj
    
    formatted = {}
    for key, value in doc.items():
        if isinstance(value, list):
            formatted[key] = [convert(v) if not isinstance(v, dict) else v for v in value]
        else:
            formatted[key] = convert(value)
    
    return json.dumps(formatted, indent=2, default=str)


def validate_collection(db, collection_name):
    """Print sample documents from a collection."""
    print(f"\n{'='*60}")
    print(f"📊 Collection: {collection_name}")
    print('='*60)
    
    try:
        collection = db[collection_name]
        count = collection.count_documents({})
        print(f"Total documents: {count}")
        
        if count == 0:
            print("  ⚠️  No documents found in this collection")
            return
        
        # Get sample documents
        print(f"\nSample documents (up to {SAMPLE_SIZE}):")
        print("-" * 40)
        
        for i, doc in enumerate(collection.find().limit(SAMPLE_SIZE), 1):
            print(f"\n[Document {i}]")
            print(format_document(doc))
            
    except Exception as e:
        print(f"  ❌ Error reading collection: {e}")


def print_collection_stats(db):
    """Print summary statistics for all collections."""
    print("\n" + "="*60)
    print("📈 Collection Statistics Summary")
    print("="*60)
    print(f"{'Collection':<25} {'Documents':>15}")
    print("-"*40)
    
    total = 0
    for collection_name in COLLECTIONS:
        try:
            count = db[collection_name].count_documents({})
            total += count
            status = "✓" if count > 0 else "○"
            print(f"{status} {collection_name:<23} {count:>15,}")
        except Exception as e:
            print(f"✗ {collection_name:<23} {'Error':>15}")
    
    print("-"*40)
    print(f"  {'Total':<23} {total:>15,}")


def main():
    """Main execution function."""
    print("="*60)
    print("MongoDB Data Validation for GlobalMart Star Schema")
    print("="*60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"MongoDB URI: {MONGODB_URI}")
    print(f"Database: {DATABASE_NAME}")
    
    # Connect to MongoDB
    try:
        print(f"\nConnecting to MongoDB...")
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        print("✓ Connected successfully")
        
        # Get replica set info
        try:
            rs_status = client.admin.command('replSetGetStatus')
            primary = next((m['name'] for m in rs_status['members'] if m['stateStr'] == 'PRIMARY'), 'Unknown')
            print(f"✓ Replica Set: {rs_status['set']}, Primary: {primary}")
        except:
            pass
            
    except Exception as e:
        print(f"❌ Failed to connect to MongoDB: {e}")
        return
    
    db = client[DATABASE_NAME]
    
    # Print statistics summary first
    print_collection_stats(db)
    
    # Validate each collection
    for collection_name in COLLECTIONS:
        validate_collection(db, collection_name)
    
    # Close connection
    client.close()
    print("\n" + "="*60)
    print("✓ Validation complete")
    print("="*60)


if __name__ == "__main__":
    main()
