import pandas as pd
from tkinter import Tk, filedialog
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError, IntegrityError, SQLAlchemyError
import numpy as np
import pyodbc
import datetime # <--- Import necesario para la fecha
import os
import sys
import re
from dotenv import load_dotenv

def get_env_path():
    """Obtiene la ruta correcta del archivo .env."""
    if getattr(sys, 'frozen', False):
        return os.path.join(sys._MEIPASS, '.env')
    else:
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
TABLE_NAME = 'Pending_Orders'
CLIENTES_TABLE_NAME = 'Clientes'

connection_string = f"mssql+pymssql://{USERNAME}:{PASSWORD}@{SERVER_AND_PORT}/{DATABASE_NAME}"
engine = create_engine(connection_string)

try:
    with engine.connect() as connection:
        connection.execute(text("SELECT 1"))
    print(f"Conexión a SQL Server '{DATABASE_NAME}' en '{SERVER_NAME}' establecida.")

    # --- Lógica para seleccionar archivo ---
    root = Tk()
    root.withdraw()
    print("Por favor, selecciona el archivo 'ordenes_pendientes.csv'...")
    file_path = filedialog.askopenfilename(
        title="Selecciona el archivo 'ordenes_pendientes.csv'",
        filetypes=[("Archivos CSV", "*.csv")]
    )
    if not file_path:
        print("No se seleccionó ningún archivo. Saliendo del programa.")
        exit()
    input_file_path = file_path

    # --- Cargar y Pre-procesar el CSV ---
    try:
        df = pd.read_csv(input_file_path, skiprows=6, skipfooter=1, engine='python')
        print("CSV cargado exitosamente.")
    except Exception as e:
        print(f"Ocurrió un error inesperado al cargar el CSV: {e}")
        exit()

    # --- Renombrar Columnas ---
    column_renames = {
        'Customer ': 'nombre_cliente',
        'Amount (Net) ': 'amount_net',
        'Document Number ': 'document_number',
        'Date ': 'fecha',
        'Class Item ': 'class_item',
        'Quantity ':'cantidad'
    }
    if 'Validated Status ' in df.columns:
        column_renames['Validated Status '] = 'estado'
    elif 'Status ' in df.columns:
        column_renames['Status '] = 'estado'
    
    df = df.rename(columns=column_renames)
    df['class_item'] = df['class_item'].fillna("Descuento")
    print("Columnas renombradas.")

    # --- Procesamiento de Fechas ---
    if 'fecha' in df.columns:
        df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce').dt.normalize()
        df['fecha'] = df['fecha'].fillna(pd.Timestamp('1900-01-01'))
        df['nombre_mes'] = df['fecha'].dt.strftime('%B')
        df['mes'] = df['fecha'].dt.month
        df['dia'] = df['fecha'].dt.day
        df['año'] = df['fecha'].dt.year
        print("Columnas de fecha procesadas.")

    # --- 6. Mapear Clientes y Zonas en un solo paso ---
    print("\nMapeando clientes y zonas desde la tabla Clientes...")
    DEFAULT_ZONE_ID = 1 # Zona por defecto si un cliente no la tiene asignada
    try:
        with engine.connect() as connection:
            # MODIFICACIÓN: Pedimos también la columna id_zone
            clientes_db_query = text(f"SELECT id_cliente, nombre_cliente, id_zone FROM {CLIENTES_TABLE_NAME};")
            clientes_db = pd.read_sql_query(clientes_db_query, connection)
        
        def clean_customer_name(name):
            if pd.isna(name): return None
            name = str(name).strip().lower()
            name = re.sub(r'[^a-z0-9\s]', '', name)
            name = re.sub(r'\s+', ' ', name).strip()
            return name

        df['nombre_cliente_cleaned'] = df['nombre_cliente'].apply(clean_customer_name)
        clientes_db['nombre_cliente_cleaned'] = clientes_db['nombre_cliente'].apply(clean_customer_name)
        
        # MODIFICACIÓN: Incluimos id_zone en el merge para traerlo a nuestro DataFrame
        df = pd.merge(df, clientes_db[['id_cliente', 'nombre_cliente_cleaned', 'id_zone']], 
                      on='nombre_cliente_cleaned', how='left')
        
        unmapped_clientes = df[df['id_cliente'].isna()]['nombre_cliente'].unique()
        if len(unmapped_clientes) > 0:
            print(f"Advertencia: Los siguientes clientes no se encontraron y se omitirán: {', '.join(map(str, unmapped_clientes))}")
        
        # Lógica de asignación de zona y limpieza
        df = df.dropna(subset=['id_cliente']).copy()
        df['id_cliente'] = df['id_cliente'].astype(int)
        
        # Si un cliente existe pero no tiene zona asignada en la DB, le ponemos la de por defecto
        df['id_zone'] = df['id_zone'].fillna(DEFAULT_ZONE_ID).astype(int)
        
        print("Mapeo de clientes y zonas finalizado.")

    except SQLAlchemyError as e:
        print(f"Error al obtener datos de la tabla Clientes: {e}")
        # Comprobar si el error es por la columna 'id_zone'
        if 'id_zone' in str(e):
            print("VERIFICA que la columna 'id_zone' exista en tu tabla 'Clientes'.")
        exit()
    
    # --- 7. Conversión Final de Tipos y Limpieza ---
    print("\nRealizando limpieza final...")
    if 'amount_net' in df.columns:
        df['amount_net'] = df['amount_net'].astype(str).str.replace('$', '', regex=False).str.replace(',', '', regex=False).str.strip()
        df['amount_net'] = pd.to_numeric(df['amount_net'], errors='coerce').fillna(0.0)

        cleaned_cantidad = df['cantidad'].astype(str).str.replace(',', '')
        df['cantidad'] = pd.to_numeric(cleaned_cantidad, errors='coerce').fillna(0).astype(int)

    if 'document_number' in df.columns:
        df['document_number'] = df['document_number'].astype(str).str.strip().str[:20].fillna('')
    
    if 'estado' in df.columns:
        df['estado'] = df['estado'].astype(str).str.strip().str[:50].fillna('Desconocido')
        
    final_db_columns = [
        'id_cliente', 'class_item', 'cantidad', 'amount_net', 'document_number', 'estado', 'fecha',
        'id_zone', 'nombre_mes', 'mes', 'dia', 'año'
    ]
    df_para_sql = df[[col for col in final_db_columns if col in df.columns]]
    print("Limpieza final completada.")
    
    # --- 8. Preparación final para la inserción ---
    df_to_insert = df_para_sql.copy()
    print(f"\nTotal de filas en el DataFrame preparado: {len(df_para_sql)}")
    print(f"Filas a insertar (snapshot diario completo): {len(df_to_insert)}")
    
    # --- 10. Insertar el DataFrame en SQL Server por lotes ---
    if len(df_to_insert) == 0:
        print(f"No hay registros válidos para insertar en la tabla '{TABLE_NAME}'. Proceso completado.")
    else:
        df_to_insert['FechaCarga'] = datetime.date.today()
        
        print(f"\nIniciando inserción por lotes en la tabla '{TABLE_NAME}'...")
        BATCH_SIZE = 1000
        try:
            with engine.connect() as connection_insert_records:
                with connection_insert_records.begin():
                    df_to_insert.to_sql(TABLE_NAME, con=connection_insert_records, if_exists='append', index=False, chunksize=BATCH_SIZE)
            print(f"\nProceso de carga de '{TABLE_NAME}' finalizado. Total de filas insertadas: {len(df_to_insert)}.")
        except (ProgrammingError, IntegrityError, SQLAlchemyError) as e:
            print(f"\n¡ERROR DURANTE LA INSERCIÓN!")
            print(f"Tipo de error: {type(e).__name__}")
            print(f"Mensaje: {e}")
            
except Exception as e:
    print(f"Ocurrió un error inesperado en el script: {e}")
finally:
    if 'engine' in locals() and engine:
        engine.dispose()
    print("Recursos de la base de datos liberados.")