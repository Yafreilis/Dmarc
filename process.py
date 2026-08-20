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
    
    total_processed_files = 0
    total_duplicate_files = 0

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
                            logger.warning(f"Reporte duplicado omitido: {report_id}")
                            total_duplicate_files += 1
                            skipped_reason = 'duplicate_report'
                            continue

                        report_db_id = rep_row['id']
                        processed_any = True
                        total_processed_files += 1

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
        
        logger.info(f"Procesamiento finalizado. Nuevos guardados: {total_processed_files} | Duplicados omitidos: {total_duplicate_files}")

if __name__ == "__main__":
    if "--list" in sys.argv:
        days = 1
        for i, arg in enumerate(sys.argv):
            if arg == "--days" and i + 1 < len(sys.argv):
                try:
                    days = int(sys.argv[i + 1])
                except ValueError:
                    pass

        print(f"\n==================================================================================")
        print(f"       REPORTE ANALÍTICO DMARC - DÍA EXACTO DE HACE {days} DÍAS")
        print(f"==================================================================================\n")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # 1. Estadísticas generales (Total reportes, Total mensajes, Mensajes exitosos)
                cur.execute("""
                    SELECT 
                        COUNT(DISTINCT r.id) as total_reports,
                        COALESCE(SUM(rec.message_count), 0) as total_messages,
                        COALESCE(SUM(CASE WHEN rec.dmarc_pass THEN rec.message_count ELSE 0 END), 0) as passed_messages
                    FROM dmarc_reports r
                    LEFT JOIN dmarc_records rec ON r.id = rec.report_id
                    WHERE r.begin_date::date = CURRENT_DATE - MAKE_INTERVAL(days := %s);
                """, (days,))
                general_stats = cur.fetchone()

                # 1.1 Conteo de reportes marcados como duplicados en la base de datos (basado en el estatus de los emails o registros omitidos)
                # Opcional: También podemos contar cuántos emails tienen status_detail = 'duplicate_report' procesados en esa fecha
                cur.execute("""
                    SELECT COUNT(*) as duplicate_emails_count
                    FROM emails 
                    WHERE status_detail = 'duplicate_report' 
                      AND processed_at::date = CURRENT_DATE - MAKE_INTERVAL(days := %s);
                """, (days,))
                dup_res = cur.fetchone()
                duplicate_count = dup_res['duplicate_emails_count'] if dup_res else 0

                # 2. Detección de nuevas IPs (IPs vistas hoy pero nunca antes en fechas pasadas)
                cur.execute("""
                    SELECT COUNT(DISTINCT rec.source_ip) as new_ips_count
                    FROM dmarc_records rec
                    JOIN dmarc_reports r ON rec.report_id = r.id
                    WHERE r.begin_date::date = CURRENT_DATE - MAKE_INTERVAL(days := %s)
                      AND rec.source_ip NOT IN (
                          SELECT DISTINCT rec2.source_ip 
                          FROM dmarc_records rec2
                          JOIN dmarc_reports r2 ON rec2.report_id = r2.id
                          WHERE r2.begin_date::date < CURRENT_DATE - MAKE_INTERVAL(days := %s)
                      );
                """, (days, days))
                new_ips_res = cur.fetchone()
                new_ips_count = new_ips_res['new_ips_count'] if new_ips_res else 0

                total_reports = general_stats['total_reports'] if general_stats else 0
                total_messages = general_stats['total_messages'] if general_stats else 0
                passed_messages = general_stats['passed_messages'] if general_stats else 0
                pass_rate = (passed_messages / total_messages * 100) if total_messages > 0 else 0.0

                print(f"📊 RESUMEN GENERAL:")
                print(f"  • Reportes procesados exitosamente: {total_reports}")
                print(f"  • Reportes omitidos por ser duplicados: {duplicate_count}")
                print(f"  • Total de mensajes analizados: {total_messages}")
                print(f"  • Tasa de éxito (DMARC Pass Rate): {pass_rate:.2f}% ({passed_messages}/{total_messages} mensajes)")
                print(f"  • Nuevas IPs detectadas en el servidor: {new_ips_count}\n")

                # 3. Desglose por políticas / disposiciones (none, quarantine, reject)
                print(f"🛡️  DESGLOSE POR SECCIÓN DE POLÍTICA (DISPOSITION):")
                print(f"  {'Disposición':<15} | {'Cantidad de Mensajes':<20} | {'Porcentaje'}")
                print(f"  " + "-" * 55)
                
                cur.execute("""
                    SELECT COALESCE(rec.disposition, 'n/a') as disposition, 
                           SUM(rec.message_count) as msg_count
                    FROM dmarc_reports r
                    JOIN dmarc_records rec ON r.id = rec.report_id
                    WHERE r.begin_date::date = CURRENT_DATE - MAKE_INTERVAL(days := %s)
                    GROUP BY rec.disposition
                    ORDER BY msg_count DESC;
                """, (days,))
                disposition_rows = cur.fetchall()

                if not disposition_rows:
                    print("  No hay registros de disposición para este período.")
                else:
                    for d in disposition_rows:
                        pct = (d['msg_count'] / total_messages * 100) if total_messages > 0 else 0.0
                        print(f"  {str(d['disposition']):<15} | {str(d['msg_count']):<20} | {pct:.2f}%")
                print()

                # 4. Top 10 IPs de origen más activas
                print(f"🏆 TOP 10 IPs DE ORIGEN (Más activas):")
                print(f"  {'IP de Origen':<18} | {'País':<6} | {'Mensajes':<10} | {'Pass Rate':<10} | {'Dominio'}")
                print(f"  " + "-" * 75)

                cur.execute("""
                    SELECT rec.source_ip, rec.source_country, 
                           SUM(rec.message_count) as total_msgs,
                           SUM(CASE WHEN rec.dmarc_pass THEN rec.message_count ELSE 0 END) as passed_msgs,
                           r.domain
                    FROM dmarc_reports r
                    JOIN dmarc_records rec ON r.id = rec.report_id
                    WHERE r.begin_date::date = CURRENT_DATE - MAKE_INTERVAL(days := %s)
                    GROUP BY rec.source_ip, rec.source_country, r.domain
                    ORDER BY total_msgs DESC
                    LIMIT 10;
                """, (days,))
                top_ips = cur.fetchall()

                if not top_ips:
                    print("  No se encontraron IPs en este período.")
                else:
                    for ip in top_ips:
                        ip_pass_rate = (ip['passed_msgs'] / ip['total_msgs'] * 100) if ip['total_msgs'] > 0 else 0.0
                        country = ip['source_country'] if ip['source_country'] else 'N/A'
                        print(f"  {str(ip['source_ip']):<18} | {str(country):<6} | {str(ip['total_msgs']):<10} | {ip_pass_rate:>6.1f}%     | {str(ip['domain'])}")
                print("\n==================================================================================\n")
    else:
        process_emails()
