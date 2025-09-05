# Librerias usadas
import pandas as pd
from openpyxl import load_workbook
import datetime
import re
from io import BytesIO
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, IntegrityError
import pyodbc
from tkinter import Tk, filedialog
import sys
import os
from dotenv import load_dotenv

def get_env_path():
    """Obtiene la ruta correcta del archivo .env."""
    if getattr(sys, 'frozen', False):
        # Estamos en un entorno PyInstaller
        # La ruta del archivo .env es relativa a la ruta temporal de la aplicación
        return os.path.join(sys._MEIPASS, '.env')
    else:
        # Estamos en un entorno de desarrollo normal
        # La ruta del archivo .env es relativa al directorio actual
        return '.env'
    
# Carga las variables de entorno desde el archivo .env
env_path = get_env_path()
load_dotenv(dotenv_path=env_path)

# --- Configuración de la Base de Datos ---
SERVER_NAME = os.environ.get("SERVER_NAME")
PORT = os.environ.get("PORT")
DATABASE_NAME = os.environ.get("DATABASE_NAME")
USERNAME = os.environ.get("DB_USERNAME")
PASSWORD = os.environ.get("DB_PASSWORD")
SERVER_AND_PORT = f"{SERVER_NAME}:{PORT}"
# --- Configuración de Tablas en la Base de Datos ---
TABLE_NAME = 'Ventas_Totales' # Nombre de tu tabla de destino
CLIENTES_TABLE_NAME = 'Clientes' # Nombre de tu tabla de clientes
#--- Conexion con la base de datos
connection_string = f"mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER_AND_PORT}/{DATABASE_NAME}"
engine = create_engine(connection_string)

# --- 1. Crear el motor de SQLAlchemy y probar la conexión ---
try:
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    print(f"Conexión a SQL Server '{DATABASE_NAME}' en '{SERVER_NAME}' establecida.")
except Exception as e:
    print(f"Error de conexión a la base de datos: {e}")
    sys.exit() # Usar sys.exit()

# --- Lógica para seleccionar archivo ---
root = Tk()
root.withdraw()
print("Por favor, selecciona el archivo")
file_path = filedialog.askopenfilename(
    title="Selecciona el archivo",
    filetypes=[
        ("Todos los soportados", "*.csv;*.xlsx;*.xls"),
        ("Archivos CSV", "*.csv"),
        ("Archivos Excel", "*.xlsx;*.xls"),
        ("Todos los archivos", "*.*")
    ]
)
if not file_path:
    print("No se seleccionó ningún archivo. Saliendo del programa.")
    sys.exit() # Usar sys.exit()

input_file_path = file_path

try:
    # Verificar que el archivo existe
    if not os.path.exists(input_file_path):
        raise FileNotFoundError(f"El archivo no se encontró en '{input_file_path}'")
    
    # Obtener la extensión del archivo
    file_extension = os.path.splitext(input_file_path)[1].lower()
    
    # Cargar el archivo según su extensión
    if file_extension == '.csv':
        df = pd.read_csv(input_file_path)
        print(f"Archivo CSV cargado exitosamente: {os.path.basename(input_file_path)}")
    elif file_extension in ['.xlsx', '.xls']:
        df = pd.read_excel(input_file_path)
        print(f"Archivo Excel cargado exitosamente: {os.path.basename(input_file_path)}")
    else:
        raise ValueError(f"Formato de archivo no soportado: {file_extension}. Solo se permiten archivos .csv, .xlsx y .xls")

except FileNotFoundError:
    print(f"Error: El archivo de entrada no se encontró en '{input_file_path}'")
    sys.exit()
except pd.errors.ParserError as e:
    print(f"¡ATENCIÓN! Error de parsing al cargar el archivo: {e}")
    print(f"Por favor, revisa el archivo de entrada '{input_file_path}'.")
    sys.exit()
except ValueError as e:
    print(f"Error: {e}")
    sys.exit()
except Exception as e:
    print(f"Ocurrió un error inesperado: {e}")
    print(f"Tipo de error: {type(e).__name__}")
    sys.exit()

column_renames = {
        'Company Name': 'nombre_cliente',
        'Date' : 'fecha',
        'Document Number' : 'document_number',
        'Type':'tipo',
        'Item':'item',
        'Description' : 'descripcion',
        'Class':'clase',
        'Quantity':'cantidad_producto',
        'UOM':'presentacion',
        'Amount':'amount',
        'Created From':'created_from',
    }

df = df.drop(columns=['Status'], errors='ignore')
df = df.rename(columns=column_renames)

if 'amount' in df.columns:
    print(df[['amount']].head())
    print(f"Tipo de datos de 'amount': {df['amount'].dtype}")
    non_numeric_values = pd.to_numeric(df['amount'], errors='coerce').isna().sum()
    print(f"Cantidad de valores no numéricos (que se harán NaN) antes de la conversión: {non_numeric_values}")
else:
    print("La columna 'amount' NO se encontró después de renombrar.")
    print(f"Columnas disponibles: {df.columns.tolist()}")

df['fecha'] = pd.to_datetime(df['fecha'], format='%m/%d/%Y')
    
# --- 6. Mapeo de nombres de cliente directamente desde la tabla Clientes ---
print("\nEstandarizando y mapeando nombre_cliente a id_cliente desde la tabla Clientes...")
    
# **Nota:** Se elimina la sección de `nombre_estandar_map` para que el mapeo sea dinámico con la base de datos.

# Cargar los clientes de la base de datos
with engine.connect() as connection_read_clientes:
    clientes_db_query = text(f"SELECT id_cliente, nombre_cliente FROM {CLIENTES_TABLE_NAME};")
    clientes_db = pd.read_sql_query(clientes_db_query, connection_read_clientes)
    # Convertir a minúsculas y quitar espacios en ambos lados para una comparación robusta
    clientes_db['nombre_cliente_lower'] = clientes_db['nombre_cliente'].str.lower().str.strip()
    cliente_id_map_db = dict(zip(clientes_db['nombre_cliente_lower'], clientes_db['id_cliente']))

# Estandarizar los nombres del CSV (solo a minúsculas y sin espacios) y luego mapear a id_cliente
df['nombre_cliente_lower'] = df['nombre_cliente'].astype(str).str.lower().str.strip()
# Ahora mapeamos directamente los nombres del CSV a los IDs de la base de datos
# No se aplica el mapeo manual, solo el mapeo contra la DB
df['id_cliente'] = df['nombre_cliente_lower'].map(cliente_id_map_db)

unmapped_clientes = df[df['id_cliente'].isna()]['nombre_cliente'].unique()
if len(unmapped_clientes) > 0:
    print(f"Advertencia: Los siguientes clientes del CSV no se encontraron en la tabla Clientes y se omitirán: {', '.join(unmapped_clientes)}")
    # Aquí se filtran las filas que no tienen un id_cliente
    df = df.dropna(subset=['id_cliente']).copy()
else:
    print("Todos los clientes del CSV fueron encontrados en la tabla Clientes.")

df['id_cliente'] = df['id_cliente'].astype(int)
print("id_cliente mapeado y clientes no encontrados manejados.")

df_para_sql = df
# --- 9. Deduplicación antes de la inserción ---
print(f"\nVerificando registros duplicados en la tabla '{TABLE_NAME}'...")

unique_cols_for_deduplication = ['id_cliente', 'fecha', 'document_number', 'item']

if not all(col in df_para_sql.columns for col in unique_cols_for_deduplication):
    print(f"¡ERROR! Las columnas para detección de duplicados no están todas presentes en df_para_sql: {unique_cols_for_deduplication}")
    print(f"Columnas disponibles: {df_para_sql.columns.tolist()}")
    raise Exception(f"Faltan columnas para la detección de duplicados en {TABLE_NAME}.")

existing_records_query_cols = ", ".join(unique_cols_for_deduplication)
existing_records_df = pd.DataFrame()
try:
    with engine.connect() as connection_read_records:
        existing_records_df = pd.read_sql_query(f"SELECT {existing_records_query_cols} FROM {TABLE_NAME}", connection_read_records)
    print(f"Se cargaron {len(existing_records_df)} filas existentes de '{TABLE_NAME}' para verificar duplicados.")
except Exception as e:
    print(f"Advertencia: No se pudieron cargar los registros existentes para la deduplicación. Procediendo sin filtrar duplicados existentes. Error: {e}")

# --- LÓGICA DE DEDUPLICACIÓN ---
df_para_sql_processed_for_dedup = df_para_sql.copy()
existing_records_df_processed_for_dedup = existing_records_df.copy()

df_para_sql_processed_for_dedup['id_cliente'] = df_para_sql_processed_for_dedup['id_cliente'].astype(int)
if not existing_records_df_processed_for_dedup.empty:
    existing_records_df_processed_for_dedup['id_cliente'] = existing_records_df_processed_for_dedup['id_cliente'].astype(int)

df_para_sql_processed_for_dedup['document_number'] = df_para_sql_processed_for_dedup['document_number'].astype(str).str.strip()
if not existing_records_df_processed_for_dedup.empty:
    existing_records_df_processed_for_dedup['document_number'] = existing_records_df_processed_for_dedup['document_number'].astype(str).str.strip()

df_para_sql_processed_for_dedup['fecha'] = pd.to_datetime(df_para_sql_processed_for_dedup['fecha']).dt.normalize()
if not existing_records_df_processed_for_dedup.empty:
    existing_records_df_processed_for_dedup['fecha'] = pd.to_datetime(existing_records_df_processed_for_dedup['fecha']).dt.normalize()

df_para_sql_processed_for_dedup['item'] = df_para_sql_processed_for_dedup['item'].astype(str).str.strip()
if not existing_records_df_processed_for_dedup.empty:
    existing_records_df_processed_for_dedup['item'] = existing_records_df_processed_for_dedup['item'].astype(str).str.strip()

existing_records_set = set(existing_records_df_processed_for_dedup[unique_cols_for_deduplication].apply(tuple, axis=1))
new_records_fingerprint = df_para_sql_processed_for_dedup[unique_cols_for_deduplication].apply(tuple, axis=1)

is_new_record = ~new_records_fingerprint.isin(existing_records_set)
df_to_insert = df_para_sql[is_new_record]
# --- FIN DE LA LÓGICA DE DEDUPLICACIÓN ---

columns_to_drop = ['nombre_cliente', 'nombre_cliente_lower']
df_to_insert = df_to_insert.drop(columns=columns_to_drop, errors='ignore')

print(f"Total de filas en el nuevo DataFrame (antes de filtrar): {len(df_para_sql)}")
print(f"Filas a insertar (nuevas y no duplicadas): {len(df_to_insert)}")
if len(df_to_insert) == 0:
    print(f"No hay nuevos registros para insertar en la tabla '{TABLE_NAME}'. Proceso completado.")
else:
    # --- 10. Insertar el DataFrame en SQL Server por lotes ---
    df_to_insert['item'] = df_to_insert['item'].astype(str)
    print(f"\nIniciando inserción por lotes de solo los datos nuevos en la tabla '{TABLE_NAME}'...")
    BATCH_SIZE = 1000 # Define el tamaño del lote
    rows_inserted_count = 0

    with engine.connect() as connection_insert_records:
        with connection_insert_records.begin(): # Usar una transacción para todo el lote
            for i in range(0, len(df_to_insert), BATCH_SIZE):
                batch_df = df_to_insert.iloc[i : i + BATCH_SIZE].copy()

                try:
                    batch_df.to_sql(TABLE_NAME, con=connection_insert_records, if_exists='append', index=False, chunksize=None)
                    rows_inserted_count += len(batch_df)
                    print(f"Lote insertado exitosamente: filas {i} a {min(i + BATCH_SIZE, len(df_to_insert))} (Total insertado: {rows_inserted_count})")

                except ProgrammingError as pe:
                    print(f"\n¡ERROR DE BASE DE DATOS en el lote de filas {i} a {min(i + BATCH_SIZE, len(df_to_insert))}!")
                    print(f"Tipo de error: {type(pe).__name__}")
                    print(f"Mensaje de error: {pe}")
                    if hasattr(pe.orig, 'args') and len(pe.orig.args) > 1:
                        print(f"    > Mensaje de SQL Server: {pe.orig.args[1]}")
                    print(f"Probable fila inicial del problema en el CSV original (aproximado): {i + 1 + 6}") # +6 por skiprows
                    print("Inspecciona los datos en tu archivo CSV cerca de esa fila o revisa tus restricciones de DB.")
                    connection_insert_records.rollback()
                    raise
                except IntegrityError as ie:
                    print(f"\n¡ERROR DE INTEGRIDAD (DUPLICADO/FK) en el lote de filas {i} a {min(i + BATCH_SIZE, len(df_to_insert))}!")
                    print(f"Tipo de error: {type(ie).__name__}")
                    print(f"Mensaje de error: {ie}")
                    if hasattr(ie.orig, 'args') and len(ie.orig.args) > 1:
                        print(f"    > Mensaje de SQL Server: {ie.orig.args[1]}")
                    print(f"Probable fila inicial del problema en el CSV original (aproximado): {i + 1 + 6}")
                    print("Esto podría indicar que un duplicado aún se está intentando insertar a pesar de la deduplicación previa, o un problema de FK.")
                    connection_insert_records.rollback()
                    raise
                except Exception as e:
                    print(f"\n¡ERROR INESPERADO en el lote de filas {i} a {min(i + BATCH_SIZE, len(df_to_insert))}!")
                    print(f"Tipo de error: {type(e).__name__}")
                    print(f"Mensaje de error: {e}")
                    print(f"Probable fila inicial del problema en el CSV original (aproximado): {i + 1 + 6}")
                    connection_insert_records.rollback()
                    raise

    print(f"\nProceso de carga de '{TABLE_NAME}' finalizado. Total de filas insertadas: {rows_inserted_count}.")