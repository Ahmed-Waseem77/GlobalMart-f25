#!/usr/bin/env python3
"""
MongoDB Flask API Gateway
-------------------------
This is a simple REST API server that sits in front of the MongoDB instance.

- It connects to MongoDB using the native binary protocol (pymongo).
- It exposes HTTP endpoints (REST) for clients to consume.
- It serializes BSON (MongoDB binary format) to JSON for the HTTP response.

Requirements:
    pip install flask pymongo
"""

from flask import Flask, request, Response, jsonify
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from bson.json_util import dumps
import sys

app = Flask(__name__)

# --- Configuration ---
# NOTE: If running locally, you may need to use 'localhost:27017' 
# or map hostnames in /etc/hosts as described previously.
MONGODB_URI = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=rs0"
DATABASE_NAME = "globalmart"

# --- Database Connection ---
try:
    client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
    # Check connection immediately on startup
    client.admin.command('ping')
    print(f"✓ Connected to MongoDB at: {MONGODB_URI}")
    db = client[DATABASE_NAME]
except ServerSelectionTimeoutError:
    print(f"❌ Error: Could not connect to MongoDB at {MONGODB_URI}")
    sys.exit(1)


@app.route('/', methods=['GET'])
def health_check():
    """Simple health check endpoint."""
    return jsonify({
        "status": "online", 
        "service": "MongoDB REST Gateway",
        "database": DATABASE_NAME
    }), 200


@app.route('/api/<collection_name>', methods=['GET'])
def get_collection_data(collection_name):
    """
    Generic endpoint to retrieve data from any collection.
    
    Query Params:
        limit (int): Number of documents to return (default: 10)
    """
    if collection_name not in db.list_collection_names():
        return jsonify({"error": "Collection not found"}), 404

    try:
        # Parse limit from query parameters
        limit = int(request.args.get('limit', 10))
        
        # Query MongoDB
        collection = db[collection_name]
        cursor = collection.find().limit(limit)
        
        # Serialize BSON -> JSON
        # dumps() from bson.json_util handles ObjectIds and Datetimes correctly
        json_response = dumps(list(cursor))
        
        # Return as 'application/json'
        return Response(json_response, mimetype='application/json', status=200)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    # Run the Flask server on port 5000
    print("✓ Starting Flask Server on port 5000...")
    app.run(host='0.0.0.0', port=5000, debug=True)
