import pandas as pd 
import sqlite3

# Lea el conjunto de datos elegido.
df = pd.read_csv('productos_vendidos.csv')

df.head(5)

# Crear DB en memoria
conn = sqlite3.connect(':memory:')
df.to_sql('Productos', conn, index=False, if_exists='replace')

# Ejecutar la consulta
query = """
SELECT product_id, SUM(quantity) AS total_vendido
FROM Productos
GROUP BY product_id
ORDER BY total_vendido DESC
LIMIT 5;
"""

result = pd.read_sql_query(query, conn)

# Guardar el resultado en un CSV

result.to_csv("top5_productos_vendidos.csv", index=False)

print("\nTop 5 productos más vendidos:")
print(result)

# Ejecutar la consulta
query = """
SELECT product_id, SUM(quantity) AS total_vendido, unit_price
FROM Productos
GROUP BY product_id
ORDER BY total_vendido DESC
LIMIT 5;
"""
result2 = pd.read_sql_query(query, conn)
print(result2)

result2.to_csv("precios_productos.csv", index=False)