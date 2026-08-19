import os
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

# Importar la función de procesamiento desde process.py
from process import process_emails

load_dotenv()

# Configuración del sistema de Logging (escribe en consola y en log_ejecucion.txt)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("log_ejecucion.txt", encoding="utf-8"),
        logging.StreamHandler()
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

def insertar_adjunto(cur, filename, contenido_bytes, sender="dmarc-reports@domain.com"):
    # Verificamos si el contenido exacto ya existe en la base de datos (evita duplicados)
    cur.execute(
        "SELECT COUNT(1) FROM email_attachments WHERE content_bytes = %s;",
        (contenido_bytes,)
    )
    existe = cur.fetchone()[0]
    
    if existe > 0:
        logging.info(f"    [Omitido] El reporte '{filename}' ya existe en la base de datos (contenido duplicado).")
        return

    # Si no existe, procedemos con la inserción normal
    unique_suffix = uuid.uuid4().hex[:8]
    message_id = f"graph-dmarc-{filename}-{unique_suffix}@local.domain"
    
    cur.execute(
        """
        INSERT INTO emails (
            internet_message_id, 
            sender, 
            recipients, 
            received_timestamptz, 
            status
        )
        VALUES (%s, %s, %s, NOW(), 'new')
        RETURNING id;
        """,
        (
            message_id, 
            sender, 
            USER_EMAIL
        )
    )
    
    row = cur.fetchone()
    email_id = row[0]

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
    
    endpoint = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages?$filter=hasAttachments eq true&$select=id,subject,sender,receivedDateTime&$top=999"
    
    response = requests.get(endpoint, headers=headers)
    if response.status_code != 200:
        logging.error(f"Error al consultar Graph API: {response.status_code} - {response.text}")
        return

    mensajes = response.json().get("value", [])
    logging.info(f"DEBUG - Total de mensajes devueltos por Graph API: {len(mensajes)}")

    for msg in mensajes:
        msg_id = msg["id"]
        sender_email = msg.get("sender", {}).get("emailAddress", {}).get("address", "unknown@domain.com")
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

        logging.info(f"Procesando correo ID: {msg_id} (De: {sender_email})")
        
        att_endpoint = f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages/{msg_id}/messages/{msg_id}/attachments" if False else f"https://graph.microsoft.com/v1.0/users/{USER_EMAIL}/messages/{msg_id}/attachments"
        att_response = requests.get(att_endpoint, headers=headers)
        
        if att_response.status_code != 200:
            logging.error(f"    [Error] No se pudieron obtener los adjuntos del mensaje {msg_id}")
            continue
            
        adjuntos = att_response.json().get("value", [])
        for att in adjuntos:
            if att.get("@odata.type") == "#microsoft.graph.fileAttachment":
                nombre_adjunto = att.get("name")
                raw_bytes = base64.b64decode(att.get("contentBytes"))
                
                nombre_extraido, contenido_bytes = procesar_contenido_bytes(nombre_adjunto, raw_bytes)
                
                if contenido_bytes:
                    nombre_personalizado = f"{sender_id}-{date_str}_{nombre_extraido}"
                    insertar_adjunto(cur, nombre_personalizado, contenido_bytes, sender=sender_email)
                else:
                    logging.info(f"    [Aviso] El adjunto '{nombre_adjunto}' fue ignorado (no es un reporte DMARC válido).")

if __name__ == "__main__":
    logging.info("Iniciando servicio automático de ingesta DMARC (modo histórico completo con logging)...")
    while True:
        try:
            logging.info("Iniciando ciclo de verificación de buzón...")
            with psycopg.connect(DATABASE_URL) as conn:
                with conn.cursor() as cur:
                    sincronizar_correos_graph(cur)
                conn.commit()
            
            # Ejecutar el procesamiento automático de los correos recién descargados ('new')
            process_emails()

            logging.info("Ciclo completado. Esperando 5 minutos para la próxima verificación...")
            
        except Exception as db_err:
            logging.error(f"[Error de conexión o ejecución]: {db_err}")
            
        time.sleep(300)
