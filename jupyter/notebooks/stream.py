import time
import json
import random
import uuid
from datetime import datetime
from kafka import KafkaProducer

# Configuration
KAFKA_BROKERS = ['kafka1:9092', 'kafka2:9092']

TOPICS = {
    'users': 'new_users',
    'transactions': 'new_transactions',
    'products': 'new_products',
    'sessions': 'new_sessions'
}

# Global pools to store generated entities for referential integrity simulation
USER_POOL = []
PRODUCT_POOL = []

def get_producer():
    """Create and return a KafkaProducer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Connected to Kafka at {KAFKA_BROKERS}")
        return producer
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

# --- Null Injector Helper ---

def inject_nulls(data):
    """
    Recursively iterates through dictionaries and lists to inject NULLs 
    based on specific field probability rules.
    """
    if isinstance(data, dict):
        # We iterate over a list of keys so we can modify the dict in place safely
        for key in list(data.keys()):
            # 1. Determine Probability based on field name
            if key == 'product_id' or key == 'session_id':
                threshold = 0.01  # 1%
            elif key in ['transaction_id', 'user_id', 'timestamp', 'products', 'events']:
                threshold = 0
            else:
                threshold = 0.005 # 0.5% for all other fields

            # 2. Apply Null Logic
            if random.random() < threshold:
                data[key] = None
            
            # 3. Recurse into nested structures (e.g., product list inside transaction)
            # Only recurse if we didn't just set the parent key to None
            elif data[key] is not None:
                if isinstance(data[key], (dict, list)):
                    inject_nulls(data[key])
                    
    elif isinstance(data, list):
        for item in data:
            inject_nulls(item)
            
    return data

# --- Data Generators ---

def generate_user():
    """Generate a random user record with realistic distributions."""
    countries = ["Egypt", "Eswatini", "Timor-Leste", "Cambodia", "Federated States of Micronesia"]
    weights = [110, 1.2, 1.3, 16, 0.1] 
    
    age = int(random.gauss(26, 10))
    age = max(18, min(90, age))
    
    domains = ["example.com", "gmail.com", "yahoo.com", "hotmail.com", "outlook.com"]
    user_id = str(uuid.uuid4())
    
    user = {
        "user_id": user_id,
        "email": f"user_{user_id}@{random.choice(domains)}",
        "age": age,
        "country": random.choices(countries, weights=weights, k=1)[0],
        "registeration_date": datetime.now().isoformat()
    }
    
    # Add clean data to pool for integrity
    USER_POOL.append(user)
    if len(USER_POOL) > 10000:
        USER_POOL.pop(0)
        
    # Return a copy with NULLs injected
    return inject_nulls(user.copy())

def generate_product():
    """Generate a random product record with category-specific biases."""
    categories = ["Electronics", "Clothing", "Home", "Books", "Sports", "Beauty", "Toys"]
    category = random.choice(categories)
    
    if category == "Electronics":
        price = round(random.uniform(100.0, 3000.0), 2)
        inventory = random.randint(10, 200)
    elif category == "Books":
        price = round(random.uniform(5.0, 50.0), 2)
        inventory = random.randint(50, 2000)
    elif category == "Clothing":
        price = round(random.uniform(10.0, 200.0), 2)
        inventory = random.randint(20, 500)
    else:
        price = round(random.uniform(10.0, 500.0), 2)
        inventory = random.randint(0, 1000)

    rating = random.gauss(4.2, 0.8)
    rating = round(max(1.0, min(5.0, rating)), 1)

    product = {
        "product_id": str(uuid.uuid4()),
        "name": f"{category}_Product_{random.randint(1, 10000)}",
        "category": category,
        "price": price,
        "inventory": inventory,
        "ratings": rating
    }
    
    # Add clean data to pool
    PRODUCT_POOL.append(product)
    if len(PRODUCT_POOL) > 5000:
        PRODUCT_POOL.pop(0)
        
    # Return a copy with NULLs injected
    return inject_nulls(product.copy())

def generate_transaction():
    """Generate a random transaction record using existing users and products if available."""
    products = []
    num_products = random.randint(1, 5)
    
    if USER_POOL:
        user_record = random.choice(USER_POOL)
        user_id = user_record["user_id"]
        user_country = user_record.get("country", "Unknown")
    else:
        user_id = str(uuid.uuid4())
        user_country = "Unknown"

    country_preferences = {
        "Egypt": ["Electronics", "Clothing"],
        "Eswatini": ["Home", "Books"],
        "Timor-Leste": ["Sports", "Beauty"],
        "Cambodia": ["Toys", "Electronics"],
        "Federated States of Micronesia": ["Clothing", "Home"]
    }

    preferred_categories = country_preferences.get(user_country, [])
    
    for _ in range(num_products):
        if PRODUCT_POOL:
            candidates = PRODUCT_POOL
            if preferred_categories and random.random() < 0.5:
                preferred_candidates = [p for p in PRODUCT_POOL if p["category"] in preferred_categories]
                if preferred_candidates:
                    candidates = preferred_candidates
            
            prod = random.choice(candidates)
            prod_id = prod["product_id"]
            price = prod["price"]
        else:
            prod_id = str(uuid.uuid4())
            price = round(random.uniform(10.0, 1000.0), 2)
            
        quantity = random.randint(1, 3)
        products.append({
            "product_id": prod_id,
            "quantity": quantity,
            "price": price
        })
    
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": user_id,
        "timestamp": datetime.now().isoformat(),
        "products": products,
        "payment_method": random.choice(["Credit Card", "PayPal", "Debit Card", "Apple Pay", "Google Pay"])
    }
    
    # Only return the injected version (no pool needed for transactions)
    return inject_nulls(transaction)

def generate_session():
    """Generate a random session record."""
    events = []
    base_time = datetime.now()
    num_events = random.randint(1, 10)
    
    user_id = random.choice(USER_POOL)["user_id"] if USER_POOL else str(uuid.uuid4())
    
    for i in range(num_events):
        events.append({
            "eventType": random.choice(["ADD_TO_CART", "REMOVE_FROM_CART", "CLEAR_CART"]),
            "timestamp": base_time.isoformat() 
        })
    
    session = {
        "user_id": user_id,
        "session_id": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "events": events
    }
    
    return inject_nulls(session)

def main():
    print("Initializing Kafka Producer...")
    producer = get_producer()
    
    if not producer:
        print("Failed to initialize producer. Please check your Kafka connection settings.")
        return

    print("Starting data stream. Press Ctrl+C to stop.")
    
    try:
        while True:
            # Produce User
            user_data = generate_user()
            producer.send(TOPICS['users'], user_data)
            
            # Produce Product
            product_data = generate_product()
            producer.send(TOPICS['products'], product_data)

            for _ in range(10): 
                # Produce Transaction
                transaction_data = generate_transaction()
                producer.send(TOPICS['transactions'], transaction_data)
            
                # Produce Session
                session_data = generate_session()
                producer.send(TOPICS['sessions'], session_data)
            
            # Flush periodically to ensure data is sent
            producer.flush()
            
            print(f"Produced 4 records (User, Product, Transaction, Session) at {datetime.now().strftime('%H:%M:%S')}")
            
            # Sleep for a random interval to simulate continuous streaming
            time.sleep(random.uniform(1, 5))

    except KeyboardInterrupt:
        print("\nStopping stream...")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        if producer:
            producer.close()
            print("Producer closed.")

if __name__ == "__main__":
    main()