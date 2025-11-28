import happybase
import sys

# Configuration
HBASE_HOST = "hbase-thrift"
HBASE_PORT = 9091

def get_hbase_connection():
    """Get HBase connection."""
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT)
        connection.open()
        return connection
    except Exception as e:
        print(f"Error connecting to HBase at {HBASE_HOST}:{HBASE_PORT}: {e}")
        # Fallback to localhost if running outside the container network but ports are mapped
        try:
            print("Attempting to connect to localhost...")
            connection = happybase.Connection(host='localhost', port=HBASE_PORT)
            connection.open()
            return connection
        except Exception as e2:
            print(f"Error connecting to HBase at localhost:{HBASE_PORT}: {e2}")
            return None

def print_table_sample(connection, table_name, limit=5):
    """Print a sample of rows from the given table."""
    print(f"\n{'='*50}")
    print(f"Table: {table_name}")
    print(f"{'='*50}")
    
    try:
        table = connection.table(table_name)
        # Scan a few rows
        count = 0
        for key, data in table.scan():
            print(f"Row Key: {key.decode('utf-8')}")
            for column, value in data.items():
                print(f"  {column.decode('utf-8')}: {value.decode('utf-8')}")
            print("-" * 30)
            count += 1
            if count >= limit:
                break
        
        if count == 0:
            print("No data found in table.")
            
    except Exception as e:
        print(f"Error reading table {table_name}: {e}")

def main():
    print("Connecting to HBase...")
    connection = get_hbase_connection()
    
    if not connection:
        print("Failed to connect to HBase. Exiting.")
        sys.exit(1)
        
    print("✓ Connected to HBase")
    
    try:
        tables = connection.tables()
        print(f"\nFound {len(tables)} tables: {[t.decode('utf-8') for t in tables]}")
        
        # Define the tables we expect to see from the ELT pipeline
        expected_tables = [
            'dim_users',
            'dim_products',
            'dim_time',
            'fact_transactions',
            'fact_sessions'
        ]
        
        for table_name in expected_tables:
            if table_name.encode() in tables:
                print_table_sample(connection, table_name)
            else:
                print(f"\nTable {table_name} not found in HBase.")
                
    except Exception as e:
        print(f"Error listing tables: {e}")
    finally:
        connection.close()

if __name__ == "__main__":
    main()
