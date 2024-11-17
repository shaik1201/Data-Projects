"""
simulator.py

This module serves as a transaction data simulator and API server.
It generates random transaction data and provides an endpoint to fetch it.

Features:
- Generate transaction data with random attributes such as price, category, etc.
- Serve the generated data via a REST API.

Author: [Shai Kiko]
Date: [2024-11-17]
"""

import random
import json
from flask import Flask, jsonify

app = Flask(__name__)

# Simulate transaction data
def generate_transaction():
    """
    Generate a random transaction object.

    Returns:
        dict: A dictionary containing transaction details such as 
              transaction_id, timestamp, user_id, product_id, category, 
              price, quantity, payment_method, and location.
    """
    
    categories = ['electronics', 'apparel', 'home', 'books', 'beauty']
    payment_methods = ['credit_card', 'paypal', 'bank_transfer']
    
    transaction = {
        "transaction_id": random.randint(100000, 999999),
        "timestamp": random.randint(1633024800, 1696153200),  # Random Unix timestamp
        "user_id": random.randint(1, 1000),
        "product_id": random.randint(1000, 9999),
        "category": random.choice(categories),
        "price": round(random.uniform(10.0, 500.0), 2),
        "quantity": random.randint(1, 5),
        "payment_method": random.choice(payment_methods),
        "location": random.choice(["New York", "San Francisco", "Los Angeles", "Chicago", "Houston"])
    }
    return transaction

@app.route('/transactions', methods=['GET'])
def transactions():
    """
    Endpoint to fetch a random transaction.

    Returns:
        JSON: A JSON object containing a randomly generated transaction.
    """
    transaction = generate_transaction()
    return jsonify(transaction)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
