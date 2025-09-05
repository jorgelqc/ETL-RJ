# Librerias usadas
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, IntegrityError, SQLAlchemyError
import pyodbc
import os
import sys
import re
from tkinter import Tk, filedialog
from dotenv import load_dotenv
from datetime import date # <--- IMPORTANTE: Asegúrate que esta línea esté al inicio

def get_env_path():
    """Obtiene la ruta correcta del archivo .env."""
    if getattr(sys, 'frozen', False):
        # Estamos en un entorno PyInstaller
        return os.path.join(sys._MEIPASS, '.env')
    else:
        # Estamos en un entorno de desarrollo normal
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
TABLE_NAME = 'Cartera' # Nombre de tu tabla de destino
CLIENTES_TABLE_NAME = 'Clientes' # Nombre de tu tabla de clientes

connection_string = f"mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER_AND_PORT}/{DATABASE_NAME}"
engine = create_engine(connection_string)

try:
    # --- 1. Crear el motor de SQLAlchemy ---
    engine = create_engine(connection_string)
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    print(f"Conexión a SQL Server '{DATABASE_NAME}' en '{SERVER_NAME}' establecida.")
except SQLAlchemyError as e:
    print(f"Error de conexión a la base de datos: {e}")
    exit()

# --- Lógica para seleccionar archivo ---
root = Tk()
root.withdraw()
print("Por favor, selecciona el archivo 'cartera.csv'...")
file_path = filedialog.askopenfilename(
    title="Selecciona el archivo 'cartera.csv'",
    filetypes=[("Archivos CSV", "*.csv")]
)

if not file_path:
    print("No se seleccionó ningún archivo. Saliendo del programa.")
    exit()

input_file_path = file_path

try:
    df = pd.read_csv(input_file_path, skipfooter=1, skiprows=6, engine='python')
    print(f"Archivo '{input_file_path}' cargado exitosamente.")
except FileNotFoundError:
    print(f"Error: El archivo de entrada no se encontró en '{input_file_path}'")
    sys.exit(1)
except Exception as e:
    print(f"Ocurrió un error inesperado al cargar el archivo: {e}")
    sys.exit(1)

column_renames = {
    'Zones for Financial Reporting ': 'zona_csv_original',
    'Customer:Project ': 'nombre_cliente',
    'Transaction Type ': 'tipo_transaccion',
    'Date ': 'fecha_facturacion',
    'Document Number ': 'document_number',
    'Due Date ': 'fecha_pago',
    'Open Balance ': 'open_balance'
}

df = df.rename(columns=column_renames)
print(f"\nColumnas disponibles después de renombrar: {df.columns.tolist()}")
df = df.drop(columns=['P.O. No. ', 'Age '], errors='ignore')

# Función de limpieza robusta para nombres de cliente
def clean_customer_name(name):
    if pd.isna(name):
        return None
    name = str(name).strip().lower()
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# --- LÓGICA ESPECIAL PARA WALMART Y AMAZON ---
condicion_1 = (df['zona_csv_original'].str.strip() == 'Walmart') & (df['nombre_cliente'].str.strip() == 'Ecommerce')
condicion_2 = (df['zona_csv_original'].str.strip() == 'Amazon') & (df['nombre_cliente'].str.strip() == 'Ecommerce')

df['zona_csv_original'] = np.where(condicion_1, 'E-Commerce', df['zona_csv_original'])
df['nombre_cliente'] = np.where(condicion_1, 'Walmart Ecommerce', df['nombre_cliente'])

df['zona_csv_original'] = np.where(condicion_2, 'E-Commerce', df['zona_csv_original'])
df['nombre_cliente'] = np.where(condicion_2, 'Amazon', df['nombre_cliente'])

df['nombre_cliente'] = df['nombre_cliente'].replace({'- no customer/project -': 'Sin Nombre'})

# --- Mapeo de clientes con la Base de Datos ---
try:
    with engine.connect() as connection:
        clientes_db_query = text(f"SELECT id_cliente, nombre_cliente, id_zone FROM {CLIENTES_TABLE_NAME};")
        clientes_db = pd.read_sql_query(clientes_db_query, connection)
    
    df['nombre_cliente_cleaned'] = df['nombre_cliente'].apply(clean_customer_name)
    clientes_db['nombre_cliente_cleaned'] = clientes_db['nombre_cliente'].apply(clean_customer_name)
    
    df = pd.merge(df, clientes_db[['id_cliente', 'id_zone', 'nombre_cliente_cleaned']], 
                  on='nombre_cliente_cleaned', how='left', suffixes=('_csv', '_db'))
    
    df['id_zone'] = df['id_zone'].fillna(df['zona_csv_original'])
    
    unmapped_clientes = df[df['id_cliente'].isna()]['nombre_cliente'].unique()
    if len(unmapped_clientes) > 0:
        print(f"Advertencia: Los siguientes clientes no se encontraron en la tabla Clientes y se omitirán: {', '.join(unmapped_clientes)}")
    else:
        print("Todos los clientes del archivo fueron encontrados.")
    
    df['id_cliente'] = pd.to_numeric(df['id_cliente'], errors='coerce')
    print("id_cliente e id_zone mapeados exitosamente.")

except SQLAlchemyError as e:
    print(f"Error al obtener clientes de la DB o al mapear: {e}")
    print("Asegúrate de que la tabla 'Clientes' existe y las columnas son correctas.")
    sys.exit(1)

# --- Proceso de limpieza de open_balance ---
if 'open_balance' in df.columns:
    print("Limpiando y convirtiendo 'open_balance'...")
    df['open_balance'] = df['open_balance'].astype(str).str.replace('(', '-', regex=False)
    df['open_balance'] = df['open_balance'].astype(str).str.replace(')', '', regex=False)
    df['open_balance'] = df['open_balance'].astype(str).str.replace('$', '', regex=False)
    df['open_balance'] = df['open_balance'].astype(str).str.replace(',', '', regex=False)
    df['open_balance'] = df['open_balance'].astype(str).str.strip()
    df['open_balance'] = pd.to_numeric(df['open_balance'], errors='coerce')
    df['open_balance'] = df['open_balance'].fillna(0)
    print("'open_balance' procesado exitosamente.")
else:
    print("La columna 'open_balance' no se encontró.")

# --- Preparación final para la inserción ---
# Usaremos todos los datos del CSV que fueron mapeados correctamente.
df_to_insert = df.dropna(subset=['id_cliente']).copy()

# Convertimos a entero DESPUÉS de eliminar los NaN para evitar errores.
df_to_insert['id_cliente'] = df_to_insert['id_cliente'].astype(int)

print(f"\nTotal de filas en el DataFrame de origen: {len(df)}")
print(f"Filas a insertar (snapshot diario completo): {len(df_to_insert)}")

# Se eliminan las columnas que ya no son necesarias para la tabla final
columns_to_drop = ['nombre_cliente', 'nombre_cliente_cleaned', 'zona_csv_original']
df_to_insert = df_to_insert.drop(columns=columns_to_drop, errors='ignore')

# Se formatean las columnas de fecha al formato YYYY-MM-DD
if 'fecha_facturacion' in df_to_insert.columns:
    df_to_insert['fecha_facturacion'] = pd.to_datetime(df_to_insert['fecha_facturacion'], errors='coerce').dt.strftime('%Y-%m-%d')
if 'fecha_pago' in df_to_insert.columns:
    df_to_insert['fecha_pago'] = pd.to_datetime(df_to_insert['fecha_pago'], errors='coerce').dt.strftime('%Y-%m-%d')

# --- Insertar en SQL Server ---
if len(df_to_insert) == 0:
    print(f"No hay nuevos registros para insertar en la tabla '{TABLE_NAME}'. Proceso completado.")
else:
    # AÑADIMOS LA FECHA DE CARGA A TODO EL LOTE
    df_to_insert['FechaCarga'] = date.today()
    
    print(f"\nIniciando inserción por lotes en la tabla '{TABLE_NAME}'...")
    BATCH_SIZE = 1000
    rows_inserted_count = 0
    try:
        with engine.connect() as connection_insert_records:
            with connection_insert_records.begin():
                for i in range(0, len(df_to_insert), BATCH_SIZE):
                    batch_df = df_to_insert.iloc[i: i + BATCH_SIZE].copy()
                    
                    # Asegúrate de que las columnas coinciden con la tabla de destino
                    # Esto es importante si la tabla SQL tiene un orden específico
                    # Por ahora, to_sql manejará el mapeo por nombre de columna
                    
                    batch_df.to_sql(TABLE_NAME, con=connection_insert_records, if_exists='append', index=False, chunksize=None)
                    rows_inserted_count += len(batch_df)
                    print(f"Lote insertado exitosamente: filas {i} a {min(i + BATCH_SIZE, len(df_to_insert))}")
        print(f"\nProceso de carga de '{TABLE_NAME}' finalizado. Total de filas insertadas: {rows_inserted_count}.")
    except (ProgrammingError, IntegrityError) as err:
        print(f"Error al insertar lote. Mensaje: {err}")
    except Exception as e:
        print(f"Ocurrió un error inesperado durante la inserción: {e}")