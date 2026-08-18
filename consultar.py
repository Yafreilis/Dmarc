import os
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def consultar_por_fecha():
    fecha_busqueda = input("Ingresa la fecha a buscar (Formato YYYY-MM-DD, ej: 2026-08-17): ")
    
    with psycopg.connect(DATABASE_URL, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, subject, status, processed_at 
                FROM emails 
                WHERE status = 'processed' 
                  AND processed_at::date = %s
                ORDER BY processed_at DESC;
            """, (fecha_busqueda,))
            
            resultados = cur.fetchall()
            
            print(f"\n--- Resultados para la fecha: {fecha_busqueda} ---")
            if not resultados:
                print("No se encontraron correos procesados en esta fecha.")
            for row in resultados:
                print(f"ID: {row['id']} | Procesado a las: {row['processed_at']} | Asunto: {row['subject']}")

if __name__ == "__main__":
    consultar_por_fecha()
    