# Sistema de Análisis de Ventas con Proceso ETL (Práctica)

## Entregables requeridos
- Script SQL de creación de base de datos y tablas (Azure SQL / SQL Server): `schema_sales_azure_sql.sql`
- Pipeline ETL en Python: `etl_sales.py`
- Diagrama ER (Mermaid): `ER_diagrama_ventas.mmd`
- Evidencia (conteo y selects): carpeta `evidence/` (generada al ejecutar el ETL)

## Flujo de trabajo (ETL)
1. **Extract**: lectura de CSVs (customers, products, orders, order_details).
2. **Transform**:
   - Eliminación de duplicados por PK (CustomerID, ProductID, OrderID).
   - Eliminación de nulos críticos.
   - Normalización: fechas (`OrderDate`), categorías, tipos numéricos (precio/stock/cantidad).
   - Validación de integridad: Orders.CustomerID debe existir; OrderDetails.OrderID y ProductID deben existir.
   - Cálculo de campos derivados: `LineTotal = Quantity * UnitPrice`.
3. **Load**: inserción en tablas respetando PK/FK.

## Cómo ejecutar (SQLite - recomendado para evidencia rápida)
1. Instala dependencias:
   ```bash
   pip install pandas
   ```
2. Coloca los CSV en la misma carpeta que `etl_sales.py` (o define variables de entorno).
3. Ejecuta:
   ```bash
   python etl_sales.py
   ```
4. Revisa evidencia:
   - `evidence/counts.txt`
   - `evidence/select_Customers.txt`, etc.

## Nota (SQL Server / Azure SQL)
Si vas a usar SQL Server, crea la BD con `schema_sales_azure_sql.sql` y luego configura:
- `CONNECTION_MODE=sqlserver`
- `SQLSERVER_CONN_STR="..."` (cadena ODBC)