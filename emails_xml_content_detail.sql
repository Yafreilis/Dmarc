SELECT 
    e.id AS email_id,
    e.internet_message_id,
    e.sender,
    e.recipients,
    e.received_timestamptz,
    e.status,
    a.filename,
    convert_from(a.content_bytes, 'UTF8') AS xml_content
FROM emails e
JOIN email_attachments a ON e.id = a.email_id
ORDER BY e.received_timestamptz DESC;