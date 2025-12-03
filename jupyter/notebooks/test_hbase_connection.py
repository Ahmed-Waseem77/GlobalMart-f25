import happybase
import sys
import time

HBASE_HOST = 'hbase-thrift'
HBASE_PORT = 9091

def test_connection(name, **kwargs):
    print(f"Testing connection: {name} ({kwargs})")
    try:
        connection = happybase.Connection(host=HBASE_HOST, port=HBASE_PORT, **kwargs)
        connection.open()
        print(f"SUCCESS: Connected with {name}")
        
        # Test 1: List tables
        tables = connection.tables()
        print(f"Tables: {tables}")
        
        # Test 2: Create table if not exists (for testing)
        table_name = 'test_table'
        if table_name.encode() not in tables:
            print(f"Creating table {table_name}...")
            connection.create_table(table_name, {'cf': dict()})
        
        # Test 3: Put data
        table = connection.table(table_name)
        print(f"Putting data into {table_name}...")
        table.put(b'row1', {b'cf:col1': b'value1'})
        print("Put successful")
        
        # Test 4: Scan data
        print(f"Scanning {table_name}...")
        count = 0
        for key, data in table.scan():
            print(f"Found row: {key}")
            count += 1
        print(f"Scan successful, found {count} rows")
        
        connection.close()
        return True
    except Exception as e:
        print(f"FAILED: {name} - {e}")
        return False

def main():
    print("Starting HBase connection tests...")
    
    # Test 1: Default (Binary, Buffered)
    test_connection("Default")
    
    # Test 2: Framed Transport
    # test_connection("Framed Transport", transport='framed')

if __name__ == "__main__":
    main()
