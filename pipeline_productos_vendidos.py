import pandas as pd
import sqlite3
from pathlib import Path


# -----------------------------
# CONFIGURACIÓN
# -----------------------------
RUTA_DATOS = Path("productos_vendidos.csv")
OUTPUT_TOP5 = Path("top5_productos_vendidos.csv")
OUTPUT_PRECIOS = Path("precios_productos.csv")


# -----------------------------
# 1. EXTRAER DATOS
# -----------------------------
def extraer_datos(ruta_csv: Path) -> pd.DataFrame:
    """Lee los datos desde un archivo CSV y devuelve un DataFrame."""
    df = pd.read_csv(ruta_csv)
    print(f"[extraer_datos] Cargadas {len(df)} filas desde '{ruta_csv}'")
    return df


# -----------------------------
# 2. CARGAR EN SQL (si procede)
# -----------------------------
def preparar_tabla(df: pd.DataFrame) -> sqlite3.Connection:
    """Carga el DataFrame en SQLite en memoria y devuelve la conexión."""
    conn = sqlite3.connect(':memory:')
    df.to_sql('Productos', conn, index=False, if_exists='replace')
    print("[preparar_tabla] Tabla 'Productos' creada en base de datos en memoria")
    return conn


# -----------------------------
# 3. ANÁLISIS 1: Top 5 por ventas
# -----------------------------
def top5_productos_vendidos(conn: sqlite3.Connection) -> pd.DataFrame:
    """Devuelve top 5 productos por cantidad vendida."""
    query = """
    SELECT product_id, SUM(quantity) AS total_vendido
    FROM Productos
    GROUP BY product_id
    ORDER BY total_vendido DESC
    LIMIT 5;
    """
    result = pd.read_sql_query(query, conn)
    print("[top5_productos_vendidos] Consulta ejecutada")
    return result


# -----------------------------
# 4. ANÁLISIS 2: Ventas + Precio
# -----------------------------
def top5_con_precios(conn: sqlite3.Connection) -> pd.DataFrame:
    """Devuelve top 5 productos más vendidos con su precio."""
    query = """
    SELECT product_id, SUM(quantity) AS total_vendido, unit_price
    FROM Productos
    GROUP BY product_id
    ORDER BY total_vendido DESC
    LIMIT 5;
    """
    result = pd.read_sql_query(query, conn)
    print("[top5_con_precios] Consulta ejecutada")
    return result


# -----------------------------
# 5. GUARDAR RESULTADOS
# -----------------------------
def guardar_resultados(df: pd.DataFrame, ruta: Path):
    df.to_csv(ruta, index=False)
    print(f"[guardar_resultados] Archivo guardado como '{ruta}'")


# -----------------------------
# 6. FUNCIÓN PRINCIPAL (PIPELINE)
# -----------------------------
def main():
    print("\n🏁 Iniciando pipeline...")

    # 1. Cargar datos
    df = extraer_datos(RUTA_DATOS)
    
    # 2. Crear base SQL en memoria
    conn = preparar_tabla(df)

    # 3. Ejecutar consultas
    result_top5 = top5_productos_vendidos(conn)
    result_precios = top5_con_precios(conn)

    # 4. Mostrar por consola
    print("\n📊 Top 5 productos más vendidos:")
    print(result_top5)

    print("\n💰 Top 5 con precio:")
    print(result_precios)

    # 5. Guardar archivos
    guardar_resultados(result_top5, OUTPUT_TOP5)
    guardar_resultados(result_precios, OUTPUT_PRECIOS)

    print("\n🎉 Pipeline ejecutado correctamente.")


# -----------------------------
# EJECUCIÓN
# -----------------------------
if __name__ == "__main__":
    main()
