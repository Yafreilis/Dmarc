import os
import sys

# Forzar codificación UTF-8 en Windows para evitar errores de charmap
if sys.platform.startswith("win"):
    import codecs
    if hasattr(sys.stdout, "detach"):
        sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())
    if hasattr(sys.stderr, "detach"):
        sys.stderr = codecs.getwriter("utf-8")(sys.stderr.detach())

import io
import uuid
import gzip
import zipfile
import tarfile
import time
import logging
import base64
from datetime import datetime, timezone, timedelta
from pathlib import Path
import psycopg
from dotenv import load_dotenv
import requests
import msal

from process import process_emails

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_PATH = os.path.join(BASE_DIR, "worker_log.txt")

# Asegurar que el archivo de log exista desde el inicio e imprimir directamente
if not os.path.exists(LOG_PATH):
    with open(LOG_PATH, "w", encoding="latin-1") as f:
        f.write(f"[{datetime.now()}] Archivo de log inicializado correctamente.\n")

class FlushFileHandler(logging.FileHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        FlushFileHandler(LOG_PATH, encoding="latin-1"),
        logging.StreamHandler(sys.stdout)
    ]
)

# Variables de entorno
DATABASE_URL = os.getenv("DATABASE_URL")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
USER_EMAIL = "J.arvis@cnzfe.gob.do"

def obtener_token_graph():
    """Obtiene el token de acceso usando MSAL mediante Client Credentials Flow."""
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = msal.ConfidentialClientApplication(
        CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET
    )
    scope = ["https://graph.microsoft.com/.default"]
    
    result = app.acquire_token_for_client(scopes=scope)
    if "access_token" in result:
        return result["access_token"]
    else:
        raise Exception(f"No se pudo obtener el token de Graph API: {result.get('error_description')}")

def es_reporte_dmarc_valido(contenido_bytes):
    """Verifica si el contenido del XML corresponde estrictamente a un reporte DMARC."""
    try:
        texto_xml = contenido_bytes.decode("utf-8", errors="ignore").lower()
        if "<feedback>" in texto_xml or "<report_metadata>" in texto_xml:
            return True
    except Exception:
        pass
    return False

def es_adjunto_dmarc_candidato(nombre_archivo):
    """Filtra si el nombre del archivo adjunto corresponde a un reporte DMARC basado en extensiones estándar."""
    nombre_lower = nombre_archivo.lower()
    extensiones_validas = (".zip", ".gz", ".tgz", ".xml", ".tar.gz")
    return nombre_lower.endswith(extensiones_validas)

def insertar_adjunto_con_id_real(cur, graph_msg_id, filename, contenido_bytes, sender="dmarc-reports@domain.com"):
    # 1. Verificar si este correo exacto (graph_msg_id) ya fue registrado en la base de datos
    cur.execute(
        "SELECT id FROM emails WHERE internet_message_id = %s LIMIT 1;",
        (graph_msg_id,)
    )
    email_row = cur.fetchone()

    if email_row:
        email_id = email_row[0]
        cur.execute(
            "SELECT 1 FROM email_attachments WHERE email_id = %s AND filename = %s LIMIT 1;",
            (email_id, filename)
        )
        if cur.fetchone():
            logging.info(f"    [Omitido] El archivo '{filename}' ya existe para este mensaje.")
            return
    else:
        # 2. Si el correo no existe, lo insertamos primero con estado 'new' y retry_count = 0
        cur.execute(
            """
            INSERT INTO emails (internet_message_id, sender, recipients, received_timestamptz, status, retry_count)
            VALUES (%s, %s, %s, NOW(), 'new', 0)
            RETURNING id;
            """,
            (graph_msg_id, sender, USER_EMAIL)
        )
        email_id = cur.fetchone()[0]

    # 3. Insertar el adjunto vinculado al ID del correo correspondiente
    cur.execute(
        """
        INSERT INTO email_attachments (email_id, filename, content_bytes)
        VALUES (%s, %s, %s);
        """,
        (email_id, filename, contenido_bytes)
    )
    logging.info(f"    [Éxito] Guardado en BD: {filename} (Remitente: {sender})")

def procesar_contenido_bytes(nombre_archivo, raw_bytes):
    """Procesa un archivo comprimido o plano en memoria y devuelve el nombre XML y su contenido solo si es DMARC válido."""
    nombre_final = nombre_archivo
    contenido_bytes = b""
    nombre_lower = nombre_archivo.lower()
    
    try:
        # 1. Archivos TAR.GZ o TGZ
        if nombre_lower.endswith(".tar.gz") or nombre_lower.endswith(".tgz"):
            with tarfile.open(fileobj=io.BytesIO(raw_bytes), mode="r:gz") as tar:
                member = next((m for m in tar.getmembers() if m.name.lower().endswith(".xml")), None)
                if member:
                    nombre_final = member.name.split("/")[-1]
                    f_extracted = tar.extractfile(member)
                    if f_extracted:
                        contenido_bytes = f_extracted.read()
                        
        # 2. Archivos GZ
        elif nombre_lower.endswith(".gz"):
            if nombre_lower.endswith(".xml.gz"):
                nombre_final = nombre_archivo[:-3]
            with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes)) as f_in:
                contenido_bytes = f_in.read()
                
        # 3. Archivos ZIP
        elif nombre_lower.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(raw_bytes)) as zf:
                xml_names = [name for name in zf.namelist() if name.lower().endswith(".xml")]
                if xml_names:
                    nombre_final = xml_names[0].split("/")[-1]
                    with zf.open(xml_names[0]) as f_in:
                        contenido_bytes = f_in.read()
                        
        # 4. XML plano u otros
        elif nombre_lower.endswith(".xml"):
            contenido_bytes = raw_bytes
            
        else:
            return None, b""
            
        # Validación final estricta de contenido DMARC
        if contenido_bytes and not es_reporte_dmarc_valido(contenido_bytes):
            logging.info(f"    [Omitido] El archivo '{nombre_archivo}' tiene extensión válida pero no es un reporte DMARC estructural.")
            return None, b""
            
    except Exception as e:
        logging.error(f"    [Error procesando archivo {nombre_archivo}]: {e}")
        return None, b""
        
    return nombre_final, contenido_bytes

def sincronizar_correos_graph(cur):
    logging.info(f"Conectando a Microsoft Graph API para el buzón: {USER_EMAIL}...")
    
    # Sincronizar automáticamente las secuencias de las tablas para evitar conflictos de ID duplicado
    try:
        cur.execute("SELECT setval(pg_get_serial_sequence('emails', 'id'), COALESCE((SELECT MAX(id) FROM emails), 0) + 1, false);")
        cur.execute("SELECT setval(pg_get_serial_sequence('email_attachments', 'id'), COALESCE((SELECT MAX(id) FROM email_attachments), 0) + 1, false);")
        cur.execute("SELECT setval(pg_get_serial_sequence('dmarc_reports', 'id'), COALESCE((SELECT MAX(id) FROM dmarc_reports), 0) + 1, false);")
    except Exception:
        pass

    token = obtener_token_graph()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Endpoint optimizado con filtro de adjuntos y selección de campos clave
    endpoint = (
        f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages"
        f"?$filter=hasAttachments eq true"
        f"&$select=id,subject,sender,receivedDateTime,hasAttachments"
        f"&$top=1000"
    )
    
    total_procesados_ciclo = 0

    # Bucle de paginación continua para procesar todo el buzón sin interrupciones
    while endpoint:
        response = requests.get(endpoint, headers=headers)
        if response.status_code != 200:
            logging.error(f"Error al consultar Graph API: {response.status_code} - {response.text}")
            break

        data = response.json()
        mensajes = data.get("value", [])
        total_procesados_ciclo += len(mensajes)
        logging.info(f"DEBUG - Página actual obtenida: {len(mensajes)} mensajes (Acumulado en este ciclo: {total_procesados_ciclo})")

        for msg in mensajes:
            msg_id = msg["id"]
            subject = msg.get("subject", "")
            sender_email = msg.get("sender", {}).get("emailAddress", {}).get("address", "unknown@domain.com")
            
            # Validación estricta para evitar reprocesar mensajes que ya están en la base de datos
            cur.execute(
                "SELECT COUNT(1) FROM emails WHERE internet_message_id = %s;",
                (msg_id,)
            )
            if cur.fetchone()[0] > 0:
                continue

            att_endpoint = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages/{msg_id}/attachments"
            att_response = requests.get(att_endpoint, headers=headers)
            
            if att_response.status_code != 200:
                logging.error(f"    [Error] No se pudieron obtener los adjuntos del mensaje {msg_id}")
                continue
                
            adjuntos = att_response.json().get("value", [])
            
            # Filtro previo: Verificar si al menos un adjunto cumple con formato DMARC antes de registrar el correo
            adjuntos_validos = [att for att in adjuntos if att.get("@odata.type") == "#microsoft.graph.fileAttachment" and es_adjunto_dmarc_candidato(att.get("name", ""))]
            
            if not adjuntos_validos:
                continue

            received_str = msg.get("receivedDateTime", "")
            sender_name_part = sender_email.split("@")[0].lower()
            sender_id = "".join(c for c in sender_name_part if c.isalnum() or c in ("_", "-", "."))
            if not sender_id:
                sender_id = "unknown"

            try:
                dt = datetime.fromisoformat(received_str.replace("Z", "+00:00"))
                date_str = dt.strftime("%d%m%y")
            except Exception:
                date_str = datetime.now().strftime("%d%m%y")

            logging.info(f"Correo con adjunto DMARC detectado ID: {msg_id} (De: {sender_email} | Asunto: {subject})")

            for att in adjuntos_validos:
                nombre_adjunto = att.get("name")
                raw_bytes = base64.b64decode(att.get("contentBytes"))
                
                nombre_extraido, contenido_bytes = procesar_contenido_bytes(nombre_adjunto, raw_bytes)
                
                if contenido_bytes:
                    nombre_personalizado = f"{sender_id}-{date_str}_{nombre_extraido}"
                    insertar_adjunto_con_id_real(cur, msg_id, nombre_personalizado, contenido_bytes, sender=sender_email)
                else:
                    logging.info(f"    [Aviso] El adjunto '{nombre_adjunto}' del correo no contenía un XML DMARC válido.")

        endpoint = data.get("@odata.nextLink")

if __name__ == "__main__":
    logging.info("Iniciando servicio automático de ingesta DMARC con control de duplicados, reintentos y paginación...")
    while True:
        try:
            logging.info("Iniciando ciclo de verificación de buzón...")
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    # 1. Reintentar correos con error que tengan menos de 3 intentos fallidos
                    cur.execute("""
                        UPDATE emails
                        SET status = 'new', 
                            retry_count = COALESCE(retry_count, 0) + 1
                        WHERE status = 'error' 
                          AND COALESCE(retry_count, 0) < 3;
                    """)
                    
                    # 2. Marcar como 'failed' definitivo aquellos que ya alcanzaron o superaron los 3 intentos
                    cur.execute("""
                        UPDATE emails
                        SET status = 'failed'
                        WHERE status = 'error' 
                          AND COALESCE(retry_count, 0) >= 3;
                    """)

                    # 3. Sincronizar nuevos correos desde Microsoft Graph
                    sincronizar_correos_graph(cur)
                conn.commit()
            
            # 4. Ejecutar el procesamiento automático de los correos ('new', incluyendo los recuperados)
            process_emails()

            logging.info("Ciclo completado. Esperando 2 minutos para la próxima verificación...")
            
        except Exception as db_err:
            logging.error(f"[Error de conexión o ejecución]: {db_err}")
            
        time.sleep(120)