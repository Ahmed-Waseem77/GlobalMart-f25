#!/usr/bin/env python3
"""
HBase Schema Creation Script for GlobalMart Star Schema

This script creates dimension and fact tables in HBase for the ELT pipeline.
The schema follows a star pattern with:
- Dimension tables: dim_users, dim_products, dim_time
- Fact tables: fact_transactions, fact_sessions

Usage:
    python create_hbase_schema.py
"""

import happybase
import sys
from datetime import datetime

# HBase connection configuration
HBASE_HOST = 'hbase-thrift'
HBASE_PORT = 9091

def get_connection():
    """Establish connection to HBase via Thrift."""
    try:
        print(f"Connecting to HBase at {HBASE_HOST}:{HBASE_PORT}...")
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
        connection.open()
        print("[SUCCESS] Connected to HBase successfully")
        return connection
    except Exception as e:
        print(f"[ERROR] Failed to connect to HBase: {e}")
        sys.exit(1)

def create_dimension_tables(connection):
    """Create dimension tables for the star schema."""
    print("\n=== Creating Dimension Tables ===")
    
    # Dimension: Users
    table_name = 'dim_users'
    try:
        if table_name.encode() in connection.tables():
            print(f"[WARNING] Table '{table_name}' already exists, dropping...")
            connection.delete_table(table_name, disable=True)
        
        connection.create_table(
            table_name,
            {
                'profile': dict(max_versions=1)  # email, age, country, registration_date
            }
        )
        print(f"[SUCCESS] Created table: {table_name}")
    except Exception as e:
        print(f"[ERROR] Error creating {table_name}: {e}")
    
    # Dimension: Products
    table_name = 'dim_products'
    try:
        if table_name.encode() in connection.tables():
            print(f"[WARNING] Table '{table_name}' already exists, dropping...")
            connection.delete_table(table_name, disable=True)
        
        connection.create_table(
            table_name,
            {
                'details': dict(max_versions=1)  # name, category, price, inventory, ratings
            }
        )
        print(f"[SUCCESS] Created table: {table_name}")
    except Exception as e:
        print(f"[ERROR] Error creating {table_name}: {e}")
    
    # Dimension: Time
    table_name = 'dim_time'
    try:
        if table_name.encode() in connection.tables():
            print(f"[WARNING] Table '{table_name}' already exists, dropping...")
            connection.delete_table(table_name, disable=True)
        
        connection.create_table(
            table_name,
            {
                'temporal': dict(max_versions=1)  # year, month, day, hour, day_of_week, quarter
            }
        )
        print(f"[SUCCESS] Created table: {table_name}")
    except Exception as e:
        print(f"[ERROR] Error creating {table_name}: {e}")

def create_fact_tables(connection):
    """Create fact tables for the star schema."""
    print("\n=== Creating Fact Tables ===")
    
    # Fact: Transactions
    table_name = 'fact_transactions'
    try:
        if table_name.encode() in connection.tables():
            print(f"[WARNING] Table '{table_name}' already exists, dropping...")
            connection.delete_table(table_name, disable=True)
        
        connection.create_table(
            table_name,
            {
                'metrics': dict(max_versions=1),   # total_amount, num_products, timestamp
                'refs': dict(max_versions=1),      # user_id, time_id, payment_method
                'products': dict(max_versions=1)   # product_<idx>_id, product_<idx>_quantity, product_<idx>_price
            }
        )
        print(f"[SUCCESS] Created table: {table_name}")
    except Exception as e:
        print(f"[ERROR] Error creating {table_name}: {e}")
    
    # Fact: Sessions
    table_name = 'fact_sessions'
    try:
        if table_name.encode() in connection.tables():
            print(f"[WARNING] Table '{table_name}' already exists, dropping...")
            connection.delete_table(table_name, disable=True)
        
        connection.create_table(
            table_name,
            {
                'metrics': dict(max_versions=1),  # num_events, session_duration, timestamp
                'refs': dict(max_versions=1),     # user_id, time_id
                'events': dict(max_versions=1)    # event_<idx>_type, event_<idx>_timestamp
            }
        )
        print(f"[SUCCESS] Created table: {table_name}")
    except Exception as e:
        print(f"[ERROR] Error creating {table_name}: {e}")

def verify_schema(connection):
    """Verify all tables were created successfully."""
    print("\n=== Verifying Schema ===")
    
    expected_tables = [
        'dim_users', 
        'dim_products', 
        'dim_time',
        'fact_transactions',
        'fact_sessions'
    ]
    
    actual_tables = [t.decode() for t in connection.tables()]
    
    print(f"\nExpected tables: {', '.join(expected_tables)}")
    print(f"Found tables: {', '.join(actual_tables)}")
    
    missing_tables = [t for t in expected_tables if t not in actual_tables]
    if missing_tables:
        print(f"[ERROR] Missing tables: {', '.join(missing_tables)}")
        return False
    
    print("\n[SUCCESS] All tables created successfully!")
    
    # Show table details
    print("\n=== Table Details ===")
    for table_name in expected_tables:
        try:
            table = connection.table(table_name)
            families = connection.table(table_name).families()
            print(f"\n{table_name}:")
            for family, props in families.items():
                print(f"  - Column Family: {family.decode()}")
        except Exception as e:
            print(f"[ERROR] Error getting details for {table_name}: {e}")
    
    return True

def main():
    """Main execution function."""
    print("=" * 60)
    print("HBase Star Schema Creation for GlobalMart")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    # Connect to HBase
    connection = get_connection()
    
    try:
        # Create dimension tables
        create_dimension_tables(connection)
        
        # Create fact tables
        create_fact_tables(connection)
        
        # Verify schema
        success = verify_schema(connection)
        
        if success:
            print("\n" + "=" * 60)
            print("[SUCCESS] Schema creation completed successfully!")
            print("=" * 60) 
            print("\nNext steps:")
            print("1. Start the Kafka producer (stream.ipynb)")
            print("2. Run the Spark ELT/ETL pipeline (spark_elt_flow.py)")
        else:
            print("\n" + "=" * 60)
            print("[ERROR] Schema creation completed with errors")
            print("=" * 60)
            
    except Exception as e:
        print(f"\n[ERROR] Unexpected error: {e}")
        raise
    finally:
        connection.close()
        print("\nHBase connection closed.")

if __name__ == "__main__":
    main()
