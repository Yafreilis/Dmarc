SELECT 
    e.sender AS remitente, 
    e.received_timestamptz AS fecha_recepcion, 
    a.filename AS archivo_reporte
FROM emails e
JOIN email_attachments a ON e.id = a.email_id
WHERE e.received_timestamptz >= CURRENT_DATE
ORDER BY e.received_timestamptz DESC;