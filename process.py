import os
import io
import sys
import logging
import tempfile
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from parsedmarc import parse_report_file

# Cargar variables de entorno
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Configurar logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("DMARC_PROCESSOR")

def get_db_connection():
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)

def process_emails():
    logger.info("Iniciando procesamiento de correos con estado 'new'...")
    
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 1. Obtener correos pendientes que tienen adjuntos
            cur.execute("""
                SELECT DISTINCT e.id, e.subject, e.received_timestamptz 
                FROM emails e
                JOIN email_attachments a ON e.id = a.email_id
                WHERE e.status = 'new'
                ORDER BY e.received_timestamptz ASC;
            """)
            emails = cur.fetchall()
            
            if not emails:
                logger.info("No hay correos nuevos con adjuntos para procesar.")
                return

            logger.info(f"Se encontraron {len(emails)} correos para evaluar.")

            for email in emails:
                email_id = email['id']
                logger.info(f"Procesando email ID: {email_id} - Asunto: {email['subject']}")

                cur.execute("""
                    SELECT id, filename, content_bytes 
                    FROM email_attachments 
                    WHERE email_id = %s;
                """, (email_id,))
                attachments = cur.fetchall()

                processed_any = False
                skipped_reason = 'not_a_dmarc_report'

                for att in attachments:
                    filename = att['filename'].lower()
                    if not (filename.endswith('.xml') or filename.endswith('.gz') or filename.endswith('.zip')):
                        continue

                    try:
                        # Guardar bytes en archivo temporal para que parsedmarc lo procese correctamente
                        file_bytes = bytes(att['content_bytes'])
                        ext = os.path.splitext(filename)[1]
                        
                        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as tmp_file:
                            tmp_file.write(file_bytes)
                            tmp_path = tmp_file.name

                        try:
                            # Procesar el archivo desde la ruta temporal
                            report_data = parse_report_file(tmp_path, offline=True)
                        finally:
                            if os.path.exists(tmp_path):
                                os.unlink(tmp_path)
                        
                        if not report_data or 'report_metadata' not in report_data:
                            continue

                        meta = report_data['report_metadata']
                        policy = report_data.get('policy_published', {})
                        
                        org_name = meta.get('org_name')
                        report_id = meta.get('report_id')
                        
                        begin_date = datetime.fromtimestamp(meta.get('date_begin', 0), tz=timezone.utc)
                        end_date = datetime.fromtimestamp(meta.get('date_end', 0), tz=timezone.utc)
                        domain = policy.get('domain')

                        # Insertar reporte
                        cur.execute("""
                            INSERT INTO dmarc_reports (
                                email_id, org_name, org_email, report_id, 
                                begin_date, end_date, domain, 
                                policy_p, policy_sp, policy_pct, raw
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (org_name, report_id) DO NOTHING
                            RETURNING id;
                        """, (
                            email_id, org_name, meta.get('email'), report_id,
                            begin_date, end_date, domain,
                            policy.get('p'), policy.get('sp'), policy.get('pct'),
                            psycopg.types.json.Json(report_data)
                        ))
                        
                        rep_row = cur.fetchone()
                        if not rep_row:
                            logger.warning(f"Reporte duplicado: {report_id}")
                            skipped_reason = 'duplicate_report'
                            continue

                        report_db_id = rep_row['id']
                        processed_any = True

                        # Insertar registros detallados
                        for rec in report_data.get('records', []):
                            row_info = rec.get('row', {})
                            auth_res = rec.get('auth_results', {})
                            spf_res = auth_res.get('spf', [{}])[0].get('result') if auth_res.get('spf') else None
                            dkim_res = auth_res.get('dkim', [{}])[0].get('result') if auth_res.get('dkim') else None
                            
                            cur.execute("""
                                INSERT INTO dmarc_records (
                                    report_id, source_ip, source_country, message_count, 
                                    disposition, spf_result, dkim_result, 
                                    spf_aligned, dkim_aligned, dmarc_pass, 
                                    header_from, envelope_from
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (
                                report_db_id, row_info.get('source_ip'), row_info.get('country'),
                                int(row_info.get('count', 0)), row_info.get('disposition'),
                                spf_res, dkim_res,
                                row_info.get('spf_alignment') == 1,
                                row_info.get('dkim_alignment') == 1,
                                (row_info.get('spf_alignment') == 1 and spf_res == 'pass') or 
                                (row_info.get('dkim_alignment') == 1 and dkim_res == 'pass'),
                                rec.get('identifiers', {}).get('header_from'),
                                rec.get('identifiers', {}).get('envelope_from')
                            ))

                    except Exception as e:
                        logger.error(f"Error procesando adjunto {att['filename']}: {e}")
                        skipped_reason = f"error: {str(e)}"

                # Actualizar estado del email
                if processed_any:
                    cur.execute("UPDATE emails SET status = 'processed', processed_at = NOW(), status_detail = NULL WHERE id = %s;", (email_id,))
                else:
                    cur.execute("UPDATE emails SET status = 'skipped', status_detail = %s, processed_at = NOW() WHERE id = %s;", (skipped_reason, email_id,))
                
                conn.commit()

if __name__ == "__main__":
    if "--list" in sys.argv:
        days = 1
        for i, arg in enumerate(sys.argv):
            if arg == "--days" and i + 1 < len(sys.argv):
                try:
                    days = int(sys.argv[i + 1])
                except ValueError:
                    pass

        print(f"\nListando reportes de los últimos {days} días...\n")
        print(f"{'ID':<5} | {'Org Name':<20} | {'Dominio':<25} | {'Inicio':<20} | {'Fin'}")
        print("-" * 90)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, org_name, domain, begin_date, end_date 
                    FROM dmarc_reports 
                    WHERE created_at >= NOW() - MAKE_INTERVAL(days := %s)
                    ORDER BY begin_date DESC;
                """, (days,))
                reports = cur.fetchall()
                
                if not reports:
                    print("No se encontraron reportes en el período especificado.")
                for r in reports:
                    print(f"{r['id']:<5} | {str(r['org_name']):<20} | {str(r['domain']):<25} | {str(r['begin_date']):<20} | {str(r['end_date'])}")
        print("\n")
    else:
        process_emails()
