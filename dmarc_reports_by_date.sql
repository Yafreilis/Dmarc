SELECT id, org_name, domain, begin_date, report_id 
FROM dmarc_reports 
WHERE begin_date >= '2026-07-31' 
ORDER BY begin_date DESC;
