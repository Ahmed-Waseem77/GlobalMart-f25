import json
from kafka import KafkaConsumer

# Configuration
KAFKA_BROKERS = ['kafka1:9092', 'kafka2:9092']
TOPICS = ['new_users', 'new_products', 'new_transactions', 'new_sessions']

def get_consumer():
    return KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BROKERS,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

print("🕵️  Listening for anomalies on all topics...")

consumer = get_consumer()

for message in consumer:
    data = message.value
    topic = message.topic

    # 1. TEST PRODUCT ANOMALIES
    if topic == 'new_products':
        if data.get('price') is not None and data['price'] < 0:
            print(f"[FOUND] Negative Price: {data['price']} (ID: {data['product_id']})")
        if data.get('inventory') is not None and data['inventory'] < 0:
            print(f"[FOUND] Negative Inventory: {data['inventory']} (ID: {data['product_id']})")
        if data.get('inventory') == 0:
            print(f"[FOUND] Zero Stock Alert (ID: {data['product_id']})")

    # 2. TEST TRANSACTION ANOMALIES
    elif topic == 'new_transactions':
        # Check for weird payment methods
        if data.get('payment_method') == "Unknown_Method":
             print(f"[FOUND] Invalid Payment Method in Transaction {data['transaction_id']}")
        
        # Check for abnormal quantities inside the products list
        if data.get('products'):
            for item in data['products']:
                if item.get('quantity') and item['quantity'] > 50:
                    print(f"[FOUND] Bulk Buying Anomaly! Qty: {item['quantity']} (Tx: {data['transaction_id']})")

    # 3. TEST USER ANOMALIES
    elif topic == 'new_users':
        if data.get('country') == "Antarctica":
             print(f"[FOUND] Suspicious User Country: Antarctica (User: {data['user_id']})")
        
        # Check simple string comparison for future dates (approximate check)
        if data.get('registeration_date') and "2026" in data['registeration_date']:
             print(f"[FOUND] Future Registration Date: {data['registeration_date']}")

    # 4. TEST SESSION ANOMALIES
    elif topic == 'new_sessions':
        if data.get('events') and len(data['events']) > 50:
            print(f"[FOUND] Bot Activity Detected! Event Count: {len(data['events'])} (Session: {data['session_id']})")