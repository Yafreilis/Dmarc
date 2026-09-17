import logging
import os
import sys
import tempfile
import gzip
import zipfile
import io
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row
from parsedmarc import parse_report_file
from lxml import etree
import re

def limpiar_xml_dmarc(contenido_bytes):
    """Limpia y estandariza etiquetas y asegura codificación UTF-8 limpia."""
    try:
        xml_text = contenido_bytes.decode('utf-8', errors='ignore')
        if not xml_text.strip():
            xml_text = contenido_bytes.decode('latin-1', errors='ignore')
            
        reemplazos = {
            '<registro>': '<record>', '</registro>': '</record>',
            '<fila>': '<row>', '</fila>': '</row>',
            '<ip_origen>': '<source_ip>', '</ip_origen>': '</source_ip>',
            '<contador>': '<count>', '</contador>': '</count>',
            '<política_evaluada>': '<policy_evaluated>', '</política_evaluada>': '</policy_evaluated>',
            '<disposición>': '<disposition>', '</disposición>': '</disposition>',
            '<auth_resultados>': '<auth_results>', '</auth_resultados>': '</auth_results>',
            '<resultados_autenticación>': '<auth_results>', '</resultados_autenticacion>': '</auth_results>',
            '<dominio>': '<domain>', '</dominio>': '</domain>',
            '<selector>': '<selector>', '</selector>': '</selector>',
            '<resultado>': '<result>', '</resultado>': '</result>',
            '<aprobado>': 'pass', '<fallido>': 'fail', '<fallo>': 'fail',
            '<ninguna>': 'none', '<cuarentena>': 'quarantine'
        }
        for esp, eng in reemplazos.items():
            xml_text = xml_text.replace(esp, eng)
        return xml_text.encode('utf-8')
    except Exception:
        return contenido_bytes
    
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

def parse_dmarc_date(value):
    """Convierte la fecha devuelta por parsedmarc (string 'YYYY-MM-DD HH:MM:SS')
    a un datetime con timezone UTC. Soporta también el caso en que ya venga
    como objeto datetime, por si una futura versión de la librería cambia el tipo."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        return dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError) as e:
        logger.warning(f"No se pudo parsear fecha '{value}': {e}")
        return None

def process_emails():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # Intentar tomar un bloqueo exclusivo global para evitar ejecuciones concurrentes
            cur.execute("SELECT pg_try_advisory_lock(987654321);")
            lock_acquired = cur.fetchone()['pg_try_advisory_lock']
            
            if not lock_acquired:
                logger.warning("Ya hay otra instancia del procesador DMARC ejecutándose. Abortando esta ejecución para evitar conflictos.")
                return

            logger.info("Iniciando procesamiento de correos con estado 'new'...")

            total_processed_files = 0
            total_duplicate_files = 0

            # Obtener correos pendientes que tienen adjuntos
            cur.execute("""
                SELECT DISTINCT e.id, e.subject, e.received_timestamptz
                FROM emails e
                WHERE e.status = 'new'
                  AND EXISTS (
                      SELECT 1 FROM email_attachments a WHERE a.email_id = e.id
                  )
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
                    if not (filename.endswith('.xml')
                            or filename.endswith('.gz')
                            or filename.endswith('.zip')):
                        continue

                    try:
                        io_bytes = io.BytesIO(att['content_bytes'])

                        raw_xml_bytes = att['content_bytes']
                        try:
                            if filename.endswith('.gz') or att['content_bytes'][:2] == b'\x1f\x8b':
                                with gzip.GzipFile(fileobj=io_bytes) as gz:
                                    raw_xml_bytes = gz.read()
                            elif filename.endswith('.zip') or att['content_bytes'][:4] == b'PK\x03\x04':
                                with zipfile.ZipFile(io_bytes) as z:
                                    for zname in z.namelist():
                                        raw_xml_bytes = z.read(zname)
                                        break
                        except Exception as ex:
                            logger.warning(f"No se pudo descomprimir automáticamente, se usará raw: {ex}")

                        raw_xml_bytes = limpiar_xml_dmarc(raw_xml_bytes)

                        if filename.endswith('.xml') or b'<' in raw_xml_bytes[:20]:
                            try:
                                etree.fromstring(raw_xml_bytes)
                            except Exception:
                                try:
                                    parser = etree.XMLParser(recover=True, encoding='utf-8')
                                    root = etree.fromstring(raw_xml_bytes, parser=parser)
                                    raw_xml_bytes = etree.tostring(
                                        root, encoding='utf-8', xml_declaration=True
                                    )
                                    logger.info(f"    XML reparado con lxml: {att['filename']}")
                                except Exception as repair_err:
                                    logger.warning(f"Reparación lxml falló, se usa original: {repair_err}")

                        xml_string = raw_xml_bytes.decode('utf-8', errors='ignore')
                        xml_string = "".join(c for c in xml_string if ord(c) < 128 or c in ('\n', '\r', '\t'))

                        tmp_path = None
                        try:
                            with tempfile.NamedTemporaryFile(delete=False, suffix=".xml", mode="w", encoding="utf-8") as tmp:
                                tmp.write(xml_string)
                                tmp_path = tmp.name
                                
                            report_data = parse_report_file(tmp_path)
                            
                            if report_data and 'report_metadata' not in report_data and 'report' in report_data:
                                report_data = report_data['report']
                                
                        except Exception as parse_err:
                            logger.error(f"Error al analizar el reporte XML: {parse_err}")
                            report_data = None
                        finally:
                            if tmp_path and os.path.exists(tmp_path):
                                os.remove(tmp_path)

                        if not report_data or 'report_metadata' not in report_data:
                            continue

                        meta = report_data['report_metadata']
                        policy = report_data.get('policy_published', {})

                        org_name = meta.get('org_name')
                        report_id = meta.get('report_id')

                        # FIX: parsedmarc 8.7.0 devuelve 'begin_date' / 'end_date' como
                        # strings "YYYY-MM-DD HH:MM:SS", no como timestamps numéricos
                        # bajo 'date_begin' / 'date_end'. Por eso antes siempre caía
                        # en el default 0 -> epoch (1970-01-01).
                        begin_date = parse_dmarc_date(meta.get('begin_date'))
                        end_date = parse_dmarc_date(meta.get('end_date'))

                        if begin_date is None or end_date is None:
                            logger.warning(
                                f"No se pudo extraer begin/end date del reporte {report_id}. "
                                f"Claves disponibles en meta: {list(meta.keys())}"
                            )

                        domain = policy.get('domain')

                        logger.info(
                            f"    [Analizado] Proveedor (Org): {org_name} "
                            f"| Report ID: {report_id}"
                        )

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
                            logger.warning(
                                f"Reporte duplicado omitido: {report_id} "
                                f"del proveedor {org_name}"
                            )
                            total_duplicate_files += 1
                            if not processed_any:
                                skipped_reason = 'duplicate_report'
                            continue

                        report_db_id = rep_row['id']
                        processed_any = True
                        total_processed_files += 1

                        records_list = report_data.get('records', [])
                        logger.info(f"   [Registros] El parser encontró {len(records_list)} registros para el reporte {report_id}")

                        for rec in records_list:
                            source_info = rec.get('source', {})
                            source_ip = source_info.get('ip_address')
                            
                            if not source_ip:
                                logger.info(f"   [Omitido] Registro sin 'source_ip'. Contenido de source_info: {source_info}")
                                continue

                            country = source_info.get('country')
                            count = int(rec.get('count', 0))
                            
                            policy_eval = rec.get('policy_evaluated', {})
                            disposition = policy_eval.get('disposition')

                            alignment = rec.get('alignment', {})
                            spf_aligned = alignment.get('spf', False)
                            dkim_aligned = alignment.get('dkim', False)
                            dmarc_pass = alignment.get('dmarc', False)

                            auth_res = rec.get('auth_results', {})
                            spf_res = auth_res.get('spf', [{}])[0].get('result') if auth_res.get('spf') else None
                            dkim_res = auth_res.get('dkim', [{}])[0].get('result') if auth_res.get('dkim') else None

                            identifiers = rec.get('identifiers', {})
                            header_from = identifiers.get('header_from')
                            envelope_from = identifiers.get('envelope_from')

                            cur.execute("""
                                INSERT INTO dmarc_records (
                                    report_id, source_ip, source_country, message_count,
                                    disposition, spf_result, dkim_result,
                                    spf_aligned, dkim_aligned, dmarc_pass,
                                    header_from, envelope_from
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (
                                report_db_id,
                                source_ip,
                                country,
                                count,
                                disposition,
                                spf_res, dkim_res,
                                spf_aligned,
                                dkim_aligned,
                                dmarc_pass,
                                header_from,
                                envelope_from
                            ))

                    except Exception as e:
                        conn.rollback()
                        logger.error(f"Error procesando adjunto {att['filename']}: {e}")
                        if not processed_any:
                            skipped_reason = f"error: {str(e)}"

                if processed_any:
                    cur.execute(
                        "UPDATE emails SET status = 'processed', "
                        "processed_at = NOW(), status_detail = NULL "
                        "WHERE id = %s;",
                        (email_id,)
                    )
                else:
                    cur.execute(
                        "UPDATE emails SET status = 'skipped', "
                        "status_detail = %s, processed_at = NOW() "
                        "WHERE id = %s;",
                        (skipped_reason, email_id)
                    )

                conn.commit()

        logger.info(
            f"Procesamiento finalizado. Nuevos guardados: "
            f"{total_processed_files} | Duplicados omitidos: {total_duplicate_files}"
        )

if __name__ == "__main__":
    if "--list" in sys.argv:
        days = 7
        output_file = None
        start_date = None
        end_date = None
        
        for i, arg in enumerate(sys.argv):
            if arg == "--days" and i + 1 < len(sys.argv):
                try:
                    days = int(sys.argv[i + 1])
                except ValueError:
                    pass
            if arg == "--output" and i + 1 < len(sys.argv):
                output_file = sys.argv[i + 1]
            if arg == "--start-date" and i + 1 < len(sys.argv):
                start_date = sys.argv[i + 1]
            if arg == "--end-date" and i + 1 < len(sys.argv):
                end_date = sys.argv[i + 1]

        def generate_report(file_obj=None):
            def write_line(text=""):
                if file_obj:
                    file_obj.write(text + "\n")
                else:
                    print(text)

            write_line("\n==================================================================================")
            if start_date and end_date:
                write_line(f"      REPORTE ANALÍTICO DMARC - DESDE {start_date} HASTA {end_date}")
            else:
                write_line(f"      REPORTE ANALÍTICO DMARC - ÚLTIMOS {days} DÍAS (RANGO)")
            write_line("==================================================================================\n")

            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    if start_date and end_date:
                        date_filter_sql = "r.begin_date::date >= %s::date AND r.begin_date::date <= %s::date"
                        email_date_filter_sql = "processed_at::date >= %s::date AND processed_at::date <= %s::date"
                        params_main = (start_date, end_date)
                        params_dup = (start_date, end_date)
                        params_ips = (start_date, end_date, start_date)
                    else:
                        date_filter_sql = "r.begin_date::date >= CURRENT_DATE - MAKE_INTERVAL(days := %s)"
                        email_date_filter_sql = "processed_at::date >= CURRENT_DATE - MAKE_INTERVAL(days := %s)"
                        params_main = (days,)
                        params_dup = (days,)
                        params_ips = (days, days)

                    # 1. Estadísticas generales
                    cur.execute(f"""
                        SELECT 
                            COUNT(DISTINCT r.id) as total_reports,
                            COALESCE(SUM(rec.message_count), 0) as total_messages,
                            COALESCE(SUM(CASE WHEN rec.dmarc_pass THEN rec.message_count ELSE 0 END), 0) as passed_messages
                        FROM dmarc_reports r
                        LEFT JOIN dmarc_records rec ON r.id = rec.report_id
                        WHERE {date_filter_sql};
                    """, params_main)
                    general_stats = cur.fetchone()

                    # 1.1 Conteo de duplicados
                    cur.execute(f"""
                        SELECT COUNT(*) as duplicate_emails_count
                        FROM emails 
                        WHERE status_detail = 'duplicate_report' 
                          AND {email_date_filter_sql};
                    """, params_dup)
                    dup_res = cur.fetchone()
                    duplicate_count = dup_res['duplicate_emails_count'] if dup_res else 0

                    # 2. Nuevas IPs
                    if start_date and end_date:
                        cur.execute("""
                            SELECT COUNT(DISTINCT rec.source_ip) as new_ips_count
                            FROM dmarc_records rec
                            JOIN dmarc_reports r ON rec.report_id = r.id
                            WHERE r.begin_date::date >= %s::date AND r.begin_date::date <= %s::date
                              AND rec.source_ip NOT IN (
                                  SELECT DISTINCT rec2.source_ip 
                                  FROM dmarc_records rec2
                                  JOIN dmarc_reports r2 ON rec2.report_id = r2.id
                                  WHERE r2.begin_date::date < %s::date
                              );
                        """, (start_date, end_date, start_date))
                    else:
                        cur.execute("""
                            SELECT COUNT(DISTINCT rec.source_ip) as new_ips_count
                            FROM dmarc_records rec
                            JOIN dmarc_reports r ON rec.report_id = r.id
                            WHERE r.begin_date::date >= CURRENT_DATE - MAKE_INTERVAL(days := %s)
                              AND rec.source_ip NOT IN (
                                  SELECT DISTINCT rec2.source_ip 
                                  FROM dmarc_records rec2
                                  JOIN dmarc_reports r2 ON rec2.report_id = r2.id
                                  WHERE r2.begin_date::date < CURRENT_DATE - MAKE_INTERVAL(days := %s)
                              );
                        """, params_ips)
                    
                    new_ips_res = cur.fetchone()
                    new_ips_count = new_ips_res['new_ips_count'] if new_ips_res else 0

                    total_reports = general_stats['total_reports'] if general_stats else 0
                    total_messages = general_stats['total_messages'] if general_stats else 0
                    passed_messages = general_stats['passed_messages'] if general_stats else 0
                    pass_rate = (passed_messages / total_messages * 100) if total_messages > 0 else 0.0

                    write_line("RESUMEN GENERAL:")
                    write_line(f"  • Reportes procesados exitosamente: {total_reports}")
                    write_line(f"  • Reportes omitidos por ser duplicados: {duplicate_count}")
                    write_line(f"  • Total de mensajes analizados: {total_messages}")
                    write_line(f"  • Tasa de éxito (DMARC Pass Rate): {pass_rate:.2f}% ({passed_messages}/{total_messages} mensajes)")
                    write_line(f"  • Nuevas IPs detectadas en el servidor: {new_ips_count}\n")

                    # 3. Desglose por políticas
                    write_line("DESGLOSE POR SECCIÓN DE POLÍTICA (DISPOSITION):")
                    write_line(f"  {'Disposición':<15} | {'Cantidad de Mensajes':<20} | {'Porcentaje'}")
                    write_line("  " + "-" * 55)
                    
                    cur.execute(f"""
                        SELECT COALESCE(NULLIF(TRIM(rec.disposition), ''), 'n/a') as disposition, 
                               SUM(rec.message_count) as msg_count
                        FROM dmarc_reports r
                        JOIN dmarc_records rec ON r.id = rec.report_id
                        WHERE {date_filter_sql}
                        GROUP BY COALESCE(NULLIF(TRIM(rec.disposition), ''), 'n/a')
                        ORDER BY msg_count DESC;
                    """, params_main)
                    disposition_rows = cur.fetchall()

                    if not disposition_rows:
                        write_line("  No hay registros de disposición para este período.")
                    else:
                        for d in disposition_rows:
                            pct = (d['msg_count'] / total_messages * 100) if total_messages > 0 else 0.0
                            write_line(f"  {str(d['disposition']):<15} | {str(d['msg_count']):<20} | {pct:.2f}%")
                    write_line()

                    # 4. Top 10 IPs
                    write_line(" TOP 10 IPs DE ORIGEN (Más activas):")
                    write_line(f"  {'IP de Origen':<18} | {'País':<6} | {'Mensajes':<10} | {'Pass Rate':<10} | {'Proveedor (Org)':<18} | {'Dominio'}")
                    write_line("  " + "-" * 85)

                    cur.execute(f"""
                        SELECT 
                            rec.source_ip,
                            COALESCE(rec.source_country, 'N/A') as country,
                            SUM(rec.message_count) as total_msg,
                            SUM(CASE WHEN rec.dmarc_pass THEN rec.message_count ELSE 0 END) as passed_msg,
                            r.org_name,
                            r.domain
                        FROM dmarc_reports r
                        JOIN dmarc_records rec ON r.id = rec.report_id
                        WHERE {date_filter_sql}
                        GROUP BY rec.source_ip, rec.source_country, r.org_name, r.domain
                        ORDER BY total_msg DESC
                        LIMIT 10;
                    """, params_main)
                    top_ips = cur.fetchall()

                    if not top_ips:
                        write_line("  No hay registros de IPs para este período.")
                    else:
                        for ip_row in top_ips:
                            ip_pass_rate = (ip_row['passed_msg'] / ip_row['total_msg'] * 100) if ip_row['total_msg'] > 0 else 0.0
                            write_line(
                                f"  {str(ip_row['source_ip']):<18} | "
                                f"{str(ip_row['country']):<6} | "
                                f"{str(ip_row['total_msg']):<10} | "
                                f"{ip_pass_rate:>8.1f}%  | "
                                f"{str(ip_row['org_name']):<18} | "
                                f"{str(ip_row['domain'])}"
                            )
                    write_line("\n==================================================================================\n")

        if output_file:
            try:
                with open(output_file, "w", encoding="utf-8") as f:
                    generate_report(f)
                print(f"Reporte guardado exitosamente en: {output_file}")
            except Exception as err:
                print(f"Error al guardar el reporte en archivo: {err}")
        else:
            generate_report()
    else:
        process_emails()