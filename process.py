import os
import io
import sys
import logging
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
                SELECT id, subject, received_timestamptz 
                FROM emails 
                WHERE status = 'new' AND has_attachments = TRUE
                ORDER BY received_timestamptz ASC;
            """)
            emails = cur.fetchall()
            
            if not emails:
                logger.info("No hay correos nuevos con adjuntos para procesar.")
                return

            logger.info(f"Se encontraron {len(emails)} correos para evaluar.")

            for email in emails:
                email_id = email['id']
                logger.info(f"Procesando email ID: {email_id} - Asunto: {email['subject']}")

                # 2. Obtener los adjuntos de este correo
                cur.execute("""
                    SELECT id, filename, content_bytes 
                    FROM email_attachments 
                    WHERE email_id = %s;
                """, (email_id,))
                attachments = cur.fetchall()

                if not attachments:
                    cur.execute("""
                        UPDATE emails 
                        SET status = 'skipped', status_detail = 'no_dmarc_attachment', processed_at = NOW() 
                        WHERE id = %s;
                    """, (email_id,))
                    conn.commit()
                    continue

                processed_any = False
                skipped_reason = 'not_a_dmarc_report'

                for att in attachments:
                    filename = att['filename'].lower()
                    # Filtrar extensiones válidas para DMARC
                    if not (filename.endswith('.xml') or filename.endswith('.gz') or filename.endswith('.zip')):
                        continue

                    try:
                        file_bytes = bytes(att['content_bytes'])
                        
                        # Parsear reporte usando parsedmarc (offline=True para evitar consultas DNS externas lentas/innecesarias)
                        report_data = parse_report_file(file_bytes, file_name=att['filename'], offline=True)
                        
                        if not report_data or 'report_metadata' not in report_data:
                            continue

                        meta = report_data['report_metadata']
                        policy = report_data.get('policy_published', {})
                        
                        org_name = meta.get('org_name')
                        report_id = meta.get('report_id')
                        
                        # Fechas del reporte
                        begin_date = datetime.fromtimestamp(meta.get('date_begin', 0), tz=timezone.utc)
                        end_date = datetime.fromtimestamp(meta.get('date_end', 0), tz=timezone.utc)
                        domain = policy.get('domain')

                        # Insertar en dmarc_reports
                        cur.execute("""
                            INSERT INTO dmarc_reports (
                                email_id, org_name, org_email, report_id, 
                                begin_date, end_date, domain, 
                                policy_p, policy_sp, policy_pct, raw
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (org_name, report_id) DO NOTHING
                            RETURNING id;
                        """, (
                            email_id,
                            org_name,
                            meta.get('email'),
                            report_id,
                            begin_date,
                            end_date,
                            domain,
                            policy.get('p'),
                            policy.get('sp'),
                            policy.get('pct'),
                            psycopg.types.json.Json(report_data)
                        ))
                        
                        rep_row = cur.fetchone()
                        
                        if not rep_row:
                            # Significa que ya existía (duplicado por UNIQUE org_name, report_id)
                            logger.warning(f"Reporte duplicado omitido: Org={org_name}, ReportID={report_id}")
                            skipped_reason = 'duplicate_report'
                            continue

                        report_db_id = rep_row['id']
                        processed_any = True

                        # Insertar los registros individuales (dmarc_records)
                        records = report_data.get('records', [])
                        for rec in records:
                            row_info = rec.get('row', {})
                            auth_res = rec.get('auth_results', {})
                            
                            # SPF y DKIM eval
                            spf_res = auth_res.get('spf', [{}])[0].get('result') if auth_res.get('spf') else None
                            dkim_res = auth_res.get('dkim', [{}])[0].get('result') if auth_res.get('dkim') else None
                            
                            # Alineaciones
                            spf_aligned = row_info.get('spf_alignment') == 1 or row_info.get('spf_alignment') is True
                            dkim_aligned = row_info.get('dkim_alignment') == 1 or row_info.get('dkim_alignment') is True
                            
                            # DMARC pass general
                            dmarc_pass = (spf_aligned and spf_res == 'pass') or (dkim_aligned and dkim_res == 'pass')

                            cur.execute("""
                                INSERT INTO dmarc_records (
                                    report_id, source_ip, source_country, message_count, 
                                    disposition, spf_result, dkim_result, 
                                    spf_aligned, dkim_aligned, dmarc_pass, 
                                    header_from, envelope_from
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (
                                report_db_id,
                                row_info.get('source_ip'),
                                row_info.get('country'),
                                int(row_info.get('count', 0)),
                                row_info.get('disposition'),
                                spf_res,
                                dkim_res,
                                spf_aligned,
                                dkim_aligned,
                                dmarc_pass,
                                rec.get('identifiers', {}).get('header_from'),
                                rec.get('identifiers', {}).get('envelope_from')
                            ))

                    except Exception as e:
                        logger.error(f"Error procesando archivo adjunto {att['filename']}: {e}")
                        skipped_reason = f"error: {str(e)}"

                # 3. Actualizar estado final del correo
                if processed_any:
                    cur.execute("""
                        UPDATE emails 
                        SET status = 'processed', processed_at = NOW(), status_detail = NULL 
                        WHERE id = %s;
                    """, (email_id,))
                    logger.info(f"Email ID {email_id} marcado como 'processed'.")
                else:
                    cur.execute("""
                        UPDATE emails 
                        SET status = 'skipped', status_detail = %s, processed_at = NOW() 
                        WHERE id = %s;
                    """, (skipped_reason, email_id,))
                    logger.info(f"Email ID {email_id} marcado como 'skipped' ({skipped_reason}).")
                
                conn.commit()

if __name__ == "__main__":
    if "--summary" in sys.argv:
        days_idx = sys.argv.index("--days") if "--days" in sys.argv else -1
        days = int(sys.argv[days_idx + 1]) if days_idx != -1 and len(sys.argv) > days_idx + 1 else 30
        
        logger.info(f"Generando resumen DMARC de los últimos {days} días...")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT domain, COUNT(*) as total_reportes 
                    FROM dmarc_reports 
                    WHERE begin_date >= NOW() - INTERVAL '%s days'
                    GROUP BY domain;
                """ % days)
                for row in cur.fetchall():
                    print(f"Dominio: {row['domain']} | Total Reportes: {row['total_reportes']}")
    
    elif "--list" in sys.argv:
        days_idx = sys.argv.index("--days") if "--days" in sys.argv else -1
        days = int(sys.argv[days_idx + 1]) if days_idx != -1 and len(sys.argv) > days_idx + 1 else 30
        
        logger.info(f"Listando reportes de los últimos {days} días...")
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, org_name, domain, begin_date, end_date 
                    FROM dmarc_reports 
                    WHERE begin_date >= NOW() - INTERVAL '%s days'
                    ORDER BY begin_date DESC;
                """ % days)
                
                print(f"{'ID':<5} | {'Org Name':<20} | {'Dominio':<20} | {'Inicio':<20} | {'Fin'}")
                print("-" * 90)
                for row in cur.fetchall():
                    print(f"{row['id']:<5} | {str(row['org_name'])[:18]:<20} | {str(row['domain'])[:18]:<20} | {str(row['begin_date'])[:19]:<20} | {str(row['end_date'])[:19]}")
    
    else:
        process_emails()
