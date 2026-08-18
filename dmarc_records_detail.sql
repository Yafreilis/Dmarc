SELECT 
    r.id AS record_id,
    rep.domain,
    rep.org_name,
    r.source_ip,
    r.source_country,
    r.message_count,
    r.dmarc_pass,
    r.spf_result,
    r.dkim_result,
    r.created_at
FROM dmarc_records r
JOIN dmarc_reports rep ON r.report_id = rep.id
ORDER BY r.created_at DESC
LIMIT 50;