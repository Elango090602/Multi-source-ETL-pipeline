# -*- coding: utf-8 -*-
"""
=============================================================================
generate_sample_data.py -- Sample Data Generator for Demo / Testing
=============================================================================
Creates realistic sample data so you can run the full pipeline
without needing live database connections.

What it creates:
  data/sample_customers.xlsx   <- Excel source file
  data/mock_orders.csv         <- Reference data (MySQL schema)
  data/mock_products.csv       <- Reference data (SQL Server schema)

Run:
    python generate_sample_data.py
=============================================================================
"""

import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

SEED = 42
random.seed(SEED)
np.random.seed(SEED)

OUTPUT_DIR = "data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

N_CUSTOMERS = 150
N_PRODUCTS  = 50
N_ORDERS    = 500

# -----------------------------------------------------------------------------
# Customers -> Excel
# -----------------------------------------------------------------------------
first_names = [
    "Alice", "Bob", "Carol", "David", "Eva", "Frank", "Grace",
    "Hank", "Iris", "Jack", "Karen", "Leo", "Mia", "Nate", "Olivia",
]
last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
    "Miller", "Davis", "Martinez", "Wilson", "Taylor", "Lee",
]
countries = ["USA", "UK", "Canada", "Australia", "Germany", "France", "India"]
segments  = ["Enterprise", "SMB", "Startup", "Individual"]

customers = pd.DataFrame({
    "Customer ID": range(1001, 1001 + N_CUSTOMERS),
    "First Name":  [random.choice(first_names) for _ in range(N_CUSTOMERS)],
    "Last Name":   [random.choice(last_names)  for _ in range(N_CUSTOMERS)],
    "Email": [
        f"{random.choice(first_names).lower()}.{random.choice(last_names).lower()}"
        f"{random.randint(1, 999)}@example.com"
        for _ in range(N_CUSTOMERS)
    ],
    "Country": [random.choice(countries) for _ in range(N_CUSTOMERS)],
    "Segment": [random.choice(segments)  for _ in range(N_CUSTOMERS)],
    "Join Date": [
        (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).strftime("%Y-%m-%d")
        for _ in range(N_CUSTOMERS)
    ],
})

# Introduce NaN values to test the cleaning logic
nan_indices = random.sample(range(N_CUSTOMERS), 10)
customers.loc[nan_indices, "Country"] = np.nan

excel_path = os.path.join(OUTPUT_DIR, "sample_customers.xlsx")
customers.to_excel(excel_path, sheet_name="Customers", index=False)
print(f"[OK] Created: {excel_path}  ({len(customers)} rows)")


# -----------------------------------------------------------------------------
# Products -> SQL Server mock reference
# -----------------------------------------------------------------------------
categories = [
    "Electronics", "Clothing", "Food & Beverage",
    "Books", "Sports", "Home & Garden",
]

products = pd.DataFrame({
    "product_id":   range(2001, 2001 + N_PRODUCTS),
    "product_name": [f"Product {chr(65 + i % 26)}{i:03d}" for i in range(N_PRODUCTS)],
    "category":     [random.choice(categories)             for _ in range(N_PRODUCTS)],
    "unit_price":   np.round(np.random.uniform(5.0, 500.0, N_PRODUCTS), 2),
    "stock_qty":    np.random.randint(0, 1000, N_PRODUCTS),
    "supplier":     [f"Supplier {chr(65 + i % 10)}"        for i in range(N_PRODUCTS)],
})

products_csv = os.path.join(OUTPUT_DIR, "mock_products.csv")
products.to_csv(products_csv, index=False)
print(f"[OK] Created: {products_csv}  ({len(products)} rows)")


# -----------------------------------------------------------------------------
# Orders -> MySQL mock reference
# -----------------------------------------------------------------------------
statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]

orders = pd.DataFrame({
    "order_id":    range(3001, 3001 + N_ORDERS),
    "customer_id": [random.randint(1001, 1000 + N_CUSTOMERS) for _ in range(N_ORDERS)],
    "product_id":  [random.randint(2001, 2000 + N_PRODUCTS)  for _ in range(N_ORDERS)],
    "quantity":    np.random.randint(1, 20, N_ORDERS),
    "unit_price":  np.round(np.random.uniform(5.0, 500.0, N_ORDERS), 2),
    "discount":    np.round(np.random.choice([0, 5, 10, 15, 20], N_ORDERS), 2),
    "order_date": [
        (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))).strftime("%Y-%m-%d")
        for _ in range(N_ORDERS)
    ],
    "status": [random.choice(statuses) for _ in range(N_ORDERS)],
})

# Introduce duplicates and NaN rows to showcase cleaning
orders = pd.concat([orders, orders.sample(15, random_state=1)], ignore_index=True)
null_rows = orders.sample(5, random_state=2).index
orders.loc[null_rows, "quantity"] = np.nan

orders_csv = os.path.join(OUTPUT_DIR, "mock_orders.csv")
orders.to_csv(orders_csv, index=False)
print(
    f"[OK] Created: {orders_csv}  "
    f"({len(orders)} rows, includes 15 duplicates + 5 null rows)"
)

# -----------------------------------------------------------------------------
print("\n" + "-" * 52)
print("  Sample data created successfully!")
print("-" * 52)
print("  Next steps:")
print("  1. Load mock_orders.csv   -> MySQL source DB  (table: orders)")
print("  2. Load mock_products.csv -> SQL Server DB    (table: products)")
print("  3. Excel file ready at: data/sample_customers.xlsx")
print("  4. Copy .env.example -> .env and fill credentials")
print("  5. Run full pipeline:  python pipeline.py")
print("  6. Run offline demo:   python demo_pipeline.py")
print("-" * 52)
