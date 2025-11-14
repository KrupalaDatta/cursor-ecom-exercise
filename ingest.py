#!/usr/bin/env python3
"""
E-commerce Data Ingestion Script

This script reads JSON files containing e-commerce data and loads them into a SQLite database.
"""

import json
import sqlite3
import os
from datetime import datetime


def create_database(db_name='ecom.db'):
    """Create SQLite database and return connection."""
    # Remove existing database if it exists
    if os.path.exists(db_name):
        os.remove(db_name)
        print(f"Removed existing database: {db_name}")
    
    conn = sqlite3.connect(db_name)
    print(f"Created database: {db_name}")
    return conn


def create_tables(conn):
    """Create all required tables in the database."""
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            phone TEXT,
            address TEXT,
            created_at TEXT
        )
    ''')
    
    # Create products table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT,
            price REAL NOT NULL,
            category TEXT,
            stock INTEGER,
            image_url TEXT,
            created_at TEXT
        )
    ''')
    
    # Create orders table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            total_amount REAL NOT NULL,
            status TEXT,
            shipping_address TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    ''')
    
    # Create order_items table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price REAL NOT NULL,
            subtotal REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(id),
            FOREIGN KEY (product_id) REFERENCES products(id)
        )
    ''')
    
    # Create payments table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            payment_method TEXT,
            amount REAL NOT NULL,
            payment_status TEXT,
            transaction_id TEXT,
            payment_date TEXT,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        )
    ''')
    
    conn.commit()
    print("Created all tables successfully")


def load_json_file(file_path):
    """Load and return data from a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            print(f"Loaded {len(data)} records from {file_path}")
            return data
    except FileNotFoundError:
        print(f"Error: File {file_path} not found!")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {file_path}: {e}")
        return None


def insert_users(conn, users_data):
    """Insert users data into the users table."""
    if not users_data:
        return
    
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO users (id, name, email, phone, address, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', [
        (
            user['id'],
            user['name'],
            user['email'],
            user.get('phone', ''),
            user.get('address', ''),
            user.get('created_at', '')
        )
        for user in users_data
    ])
    conn.commit()
    print(f"Inserted {len(users_data)} users")


def insert_products(conn, products_data):
    """Insert products data into the products table."""
    if not products_data:
        return
    
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO products (id, name, description, price, category, stock, image_url, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', [
        (
            product['id'],
            product['name'],
            product.get('description', ''),
            product['price'],
            product.get('category', ''),
            product.get('stock', 0),
            product.get('image_url', ''),
            product.get('created_at', '')
        )
        for product in products_data
    ])
    conn.commit()
    print(f"Inserted {len(products_data)} products")


def insert_orders(conn, orders_data):
    """Insert orders data into the orders table."""
    if not orders_data:
        return
    
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO orders (id, user_id, order_date, total_amount, status, shipping_address)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', [
        (
            order['id'],
            order['user_id'],
            order['order_date'],
            order['total_amount'],
            order.get('status', ''),
            order.get('shipping_address', '')
        )
        for order in orders_data
    ])
    conn.commit()
    print(f"Inserted {len(orders_data)} orders")


def insert_order_items(conn, order_items_data):
    """Insert order items data into the order_items table."""
    if not order_items_data:
        return
    
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO order_items (id, order_id, product_id, quantity, price, subtotal)
        VALUES (?, ?, ?, ?, ?, ?)
    ''', [
        (
            item['id'],
            item['order_id'],
            item['product_id'],
            item['quantity'],
            item['price'],
            item['subtotal']
        )
        for item in order_items_data
    ])
    conn.commit()
    print(f"Inserted {len(order_items_data)} order items")


def insert_payments(conn, payments_data):
    """Insert payments data into the payments table."""
    if not payments_data:
        return
    
    cursor = conn.cursor()
    cursor.executemany('''
        INSERT INTO payments (id, order_id, payment_method, amount, payment_status, transaction_id, payment_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', [
        (
            payment['id'],
            payment['order_id'],
            payment.get('payment_method', ''),
            payment['amount'],
            payment.get('payment_status', ''),
            payment.get('transaction_id', ''),
            payment.get('payment_date')  # Can be None
        )
        for payment in payments_data
    ])
    conn.commit()
    print(f"Inserted {len(payments_data)} payments")


def verify_data(conn):
    """Verify the data was inserted correctly by counting records."""
    cursor = conn.cursor()
    
    tables = ['users', 'products', 'orders', 'order_items', 'payments']
    print("\n" + "="*50)
    print("Data Verification:")
    print("="*50)
    
    for table in tables:
        cursor.execute(f'SELECT COUNT(*) FROM {table}')
        count = cursor.fetchone()[0]
        print(f"{table:15} : {count:4} records")
    
    print("="*50)


def main():
    """Main function to orchestrate the data ingestion process."""
    print("="*50)
    print("E-commerce Data Ingestion Script")
    print("="*50)
    print()
    
    # File paths
    json_files = {
        'users': 'users.json',
        'products': 'products.json',
        'orders': 'orders.json',
        'order_items': 'order_items.json',
        'payments': 'payments.json'
    }
    
    # Create database connection
    conn = create_database('ecom.db')
    
    try:
        # Create tables
        print("\nCreating database tables...")
        create_tables(conn)
        
        # Load and insert data in the correct order (respecting foreign key constraints)
        print("\nLoading and inserting data...")
        print("-" * 50)
        
        # 1. Users (no dependencies)
        users_data = load_json_file(json_files['users'])
        if users_data:
            insert_users(conn, users_data)
        
        # 2. Products (no dependencies)
        products_data = load_json_file(json_files['products'])
        if products_data:
            insert_products(conn, products_data)
        
        # 3. Orders (depends on users)
        orders_data = load_json_file(json_files['orders'])
        if orders_data:
            insert_orders(conn, orders_data)
        
        # 4. Order Items (depends on orders and products)
        order_items_data = load_json_file(json_files['order_items'])
        if order_items_data:
            insert_order_items(conn, order_items_data)
        
        # 5. Payments (depends on orders)
        payments_data = load_json_file(json_files['payments'])
        if payments_data:
            insert_payments(conn, payments_data)
        
        # Verify data
        verify_data(conn)
        
        print("\n" + "="*50)
        print("Data ingestion completed successfully!")
        print("="*50)
        
    except Exception as e:
        print(f"\nError during data ingestion: {e}")
        conn.rollback()
        raise
    
    finally:
        conn.close()
        print("\nDatabase connection closed.")


if __name__ == '__main__':
    main()


