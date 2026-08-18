import os
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

# Datos locales
LOCAL_URL = "postgresql://postgres:alexander17@127.0.0.1:5432/Dmarc"
# Cadena de conexión de Neon
NEON_URL = "postgresql://neondb_owner:npg_GnvO8t9mxeZL@ep-falling-lake-ay1jvnlt-pooler.c-5.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

print("Conectando a las bases de datos...")
local_conn = psycopg2.connect(LOCAL_URL)
local_cursor = local_conn.cursor()

neon_conn = psycopg2.connect(NEON_URL)
neon_cursor = neon_conn.cursor()

# Definimos el orden manualmente para respetar las llaves foráneas
tables = ['sync_state', 'emails', 'dmarc_reports', 'email_attachments', 'dmarc_records']

print(f"Tablas a migrar: {tables}")

for table in tables:
    print(f"Migrando tabla: {table}...")
    
    # Leer datos locales
    local_cursor.execute(sql.SQL("SELECT * FROM {}").format(sql.Identifier(table)))
    rows = local_cursor.fetchall()
    
    if not rows:
        continue

    # Obtener nombres de columnas
    col_names = [desc[0] for desc in local_cursor.description]
    
    # Preparar query
    placeholders = sql.SQL(", ").join([sql.Placeholder()] * len(col_names))
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(table),
        sql.SQL(", ").join([sql.Identifier(n) for n in col_names]),
        placeholders,
    )

    for row in rows:
        try:
            # Convertir la fila en una lista para poder modificarla si tiene JSON
            data = list(row)
            
            # Si es la tabla dmarc_reports, buscamos la columna 'raw' y la envolvemos en Json()
            if table == 'dmarc_reports':
                raw_idx = col_names.index('raw')
                data[raw_idx] = Json(data[raw_idx])
            
            neon_cursor.execute(query, data)
        except Exception as e:
            print(f"Error al insertar fila en {table}: {e}")
            neon_conn.rollback()
            break # Detener si hay error para no seguir intentando con datos corruptos
    
    neon_conn.commit()

print("¡Migración de datos completada con éxito!")

local_cursor.close()
local_conn.close()
neon_cursor.close()
neon_conn.close()
