"""
ETL - Sistema de Análisis de Ventas (CSV -> Transform -> DB)

Por defecto use SQLite para  poder jecutar localmente sin instalar un servidor.

"""

from __future__ import annotations
import os
import sqlite3
import pandas as pd
from datetime import datetime

# Configuración
CONNECTION_MODE = os.getenv("CONNECTION_MODE", "sqlite")  # "sqlite" o "sqlserver"

SQLITE_DB = os.getenv("SQLITE_DB", "sales_analytics.db")

# Ejemplo de cadena (Windows):
# DRIVER={ODBC Driver 18 for SQL Server};SERVER=localhost;DATABASE=SalesAnalytics;Trusted_Connection=yes;TrustServerCertificate=yes;
CONN_STR = os.getenv("SQLSERVER_CONN_STR", "")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_CUSTOMERS = os.getenv("CSV_CUSTOMERS", os.path.join(BASE_DIR, "customers.csv"))
CSV_PRODUCTS = os.getenv("CSV_PRODUCTS", os.path.join(BASE_DIR, "products.csv"))
CSV_ORDERS = os.getenv("CSV_ORDERS", os.path.join(BASE_DIR, "orders.csv"))
CSV_ORDER_DETAILS = os.getenv("CSV_ORDER_DETAILS", os.path.join(BASE_DIR, "order_details.csv"))

#  conexión

def get_connection():
    if CONNECTION_MODE.lower() == "sqlite":
        conn = sqlite3.connect(SQLITE_DB)
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn
    elif CONNECTION_MODE.lower() == "sqlserver":
        import pyodbc
        if not CONN_STR:
            raise ValueError("Falta SQLSERVER_CONN_STR para modo sqlserver.")
        return pyodbc.connect(CONN_STR)
    else:
        raise ValueError("CONNECTION_MODE debe ser 'sqlite' o 'sqlserver'.")

def run_schema_sqlite(conn):
    """Crea las tablas en SQLite (para evidencia rápida)."""
    schema_path = os.path.join(BASE_DIR, "schema_sqlite.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        conn.executescript(f.read())

# Extracción
def extract():
    customers = pd.read_csv(CSV_CUSTOMERS)
    products = pd.read_csv(CSV_PRODUCTS)
    orders = pd.read_csv(CSV_ORDERS)
    order_details = pd.read_csv(CSV_ORDER_DETAILS)
    return customers, products, orders, order_details

# Transformación (reglas de limpieza)

def transform(customers, products, orders, order_details):
    # --- Customers ---
    customers = customers.drop_duplicates(subset=["CustomerID"]).copy()
    # nulos críticos
    customers = customers.dropna(subset=["CustomerID", "FirstName", "LastName"])
    customers["Email"] = customers["Email"].astype(str).where(customers["Email"].notna(), None)
    customers["Phone"] = customers["Phone"].astype(str).where(customers["Phone"].notna(), None)
    customers["City"] = customers["City"].astype(str).where(customers["City"].notna(), None)
    customers["Country"] = customers["Country"].astype(str).where(customers["Country"].notna(), None)

    # --- Products ---
    products = products.drop_duplicates(subset=["ProductID"]).copy()
    products = products.dropna(subset=["ProductID", "ProductName", "Price"])
    products["Price"] = pd.to_numeric(products["Price"], errors="coerce")
    products = products.dropna(subset=["Price"])
    products = products[products["Price"] >= 0]
    products["Stock"] = pd.to_numeric(products.get("Stock", 0), errors="coerce").fillna(0).astype(int)
    products.loc[products["Stock"] < 0, "Stock"] = 0
    products["Category"] = products["Category"].astype(str).where(products["Category"].notna(), "Sin categoría")

    # --- Orders ---
    orders = orders.drop_duplicates(subset=["OrderID"]).copy()
    orders = orders.dropna(subset=["OrderID", "CustomerID", "OrderDate"])
    # normalizar fecha
    orders["OrderDate"] = pd.to_datetime(orders["OrderDate"], errors="coerce").dt.date.astype(str)
    orders = orders.dropna(subset=["OrderDate"])
    orders["Status"] = orders["Status"].astype(str).where(orders["Status"].notna(), None)

    # --- OrderDetails ---
    # duplicados: si existe la misma combinación OrderID+ProductID, se suman cantidades
    order_details = order_details.dropna(subset=["OrderID", "ProductID", "Quantity"]).copy()
    order_details["Quantity"] = pd.to_numeric(order_details["Quantity"], errors="coerce")
    order_details = order_details.dropna(subset=["Quantity"])
    order_details = order_details[order_details["Quantity"] > 0]
    order_details["ProductID"] = pd.to_numeric(order_details["ProductID"], errors="coerce").astype(int)
    order_details["OrderID"] = pd.to_numeric(order_details["OrderID"], errors="coerce").astype(int)

    order_details = (order_details
                     .groupby(["OrderID", "ProductID"], as_index=False)
                     .agg({"Quantity":"sum"}))

    # --- Validación integridad referencial ---
    valid_customers = set(customers["CustomerID"].astype(int).tolist())
    valid_products = set(products["ProductID"].astype(int).tolist())
    valid_orders = set(orders["OrderID"].astype(int).tolist())

    # orders debe referenciar cliente existente
    orders = orders[orders["CustomerID"].astype(int).isin(valid_customers)]
    valid_orders = set(orders["OrderID"].astype(int).tolist())

    # order_details debe referenciar order y product existentes
    order_details = order_details[
        order_details["OrderID"].isin(valid_orders) &
        order_details["ProductID"].isin(valid_products)
    ].copy()

    # Calcular UnitPrice (desde Products) y LineTotal
    price_lookup = products.set_index("ProductID")["Price"].to_dict()
    order_details["UnitPrice"] = order_details["ProductID"].map(price_lookup).astype(float)
    order_details["LineTotal"] = (order_details["Quantity"].astype(float) * order_details["UnitPrice"].astype(float))

    return customers, products, orders, order_details

# Carga
def load(conn, customers, products, orders, order_details):
    now = datetime.utcnow().isoformat(timespec="seconds")

    # DataSources: registramos estos CSV como fuente
    # SQLite: usamos ? placeholders; SQL Server: también funciona con pyodbc si ejecutas por cursor.execute
    cur = conn.cursor()
    cur.execute("INSERT INTO DataSources(SourceName, SourceType, SourcePath, LoadedAt) VALUES (?,?,?,?)",
                ("CSV_Practica_Ventas", "CSV", "customers/products/orders/order_details", now))
    source_id = cur.lastrowid if hasattr(cur, "lastrowid") else None

    # Insert Customers
    customers = customers.copy()
    customers["SourceID"] = source_id
    cur.executemany(
        "INSERT INTO Customers(CustomerID, FirstName, LastName, Email, Phone, City, Country, SourceID) VALUES (?,?,?,?,?,?,?,?)",
        customers[["CustomerID","FirstName","LastName","Email","Phone","City","Country","SourceID"]].itertuples(index=False, name=None)
    )

    # Insert Products
    products = products.copy()
    products["SourceID"] = source_id
    cur.executemany(
        "INSERT INTO Products(ProductID, ProductName, Category, Price, Stock, SourceID) VALUES (?,?,?,?,?,?)",
        products[["ProductID","ProductName","Category","Price","Stock","SourceID"]].itertuples(index=False, name=None)
    )

    # Insert Orders
    orders = orders.copy()
    orders["SourceID"] = source_id
    cur.executemany(
        "INSERT INTO Orders(OrderID, CustomerID, OrderDate, Status, SourceID) VALUES (?,?,?,?,?)",
        orders[["OrderID","CustomerID","OrderDate","Status","SourceID"]].itertuples(index=False, name=None)
    )

    # Insert OrderDetails
    order_details = order_details.copy()
    order_details["SourceID"] = source_id
    cur.executemany(
        "INSERT INTO OrderDetails(OrderID, ProductID, Quantity, UnitPrice, LineTotal, SourceID) VALUES (?,?,?,?,?,?)",
        order_details[["OrderID","ProductID","Quantity","UnitPrice","LineTotal","SourceID"]].itertuples(index=False, name=None)
    )

    conn.commit()

def evidence(conn, out_dir="evidence"):
    os.makedirs(out_dir, exist_ok=True)
    cur = conn.cursor()

    # Conteo de registros por tabla
    counts = {}
    for t in ["DataSources","Customers","Products","Orders","OrderDetails"]:
        cur.execute(f"SELECT COUNT(*) FROM {t}")
        counts[t] = cur.fetchone()[0]
    with open(os.path.join(out_dir, "counts.txt"), "w", encoding="utf-8") as f:
        for k,v in counts.items():
            f.write(f"{k}: {v}\n")

    # Result set: 5 filas por tabla
    for t in ["Customers","Products","Orders","OrderDetails"]:
        cur.execute(f"SELECT * FROM {t} LIMIT 5")
        rows = cur.fetchall()
        with open(os.path.join(out_dir, f"select_{t}.txt"), "w", encoding="utf-8") as f:
            for r in rows:
                f.write(str(r) + "\n")

def main():
    customers, products, orders, order_details = extract()

    conn = get_connection()
    if CONNECTION_MODE.lower() == "sqlite":
        run_schema_sqlite(conn)

    customers, products, orders, order_details = transform(customers, products, orders, order_details)
    load(conn, customers, products, orders, order_details)
    evidence(conn)
    print("ETL completado. Evidencia guardada en carpeta /evidence")

if __name__ == "__main__":
    main()