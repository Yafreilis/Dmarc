SELECT 
    e.status,
    e.sender AS remitente,
    e.received_timestamptz AS fecha_envio,
    a.filename AS archivo
FROM emails e
LEFT JOIN email_attachments a ON e.id = a.email_id
ORDER BY e.received_timestamptz DESC;