SELECT source_ip, source_country, SUM(message_count) AS total_mensajes 
FROM dmarc_records 
GROUP BY source_ip, source_country 
ORDER BY total_mensajes DESC;