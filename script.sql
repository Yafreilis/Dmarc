-- 1. Tabla principal de correos... Contrato compartido Jarvis Módulo 1.
CREATE TABLE emails (
    id BIGSERIAL PRIMARY KEY,
    internet_message_id TEXT NOT NULL UNIQUE,
    subject TEXT,
    sender TEXT,
    recipients TEXT,
    received_timestamptz TIMESTAMPTZ NOT NULL,
    body_content TEXT,
    body_content_type TEXT, -- 'text' o 'html'
    has_attachments BOOLEAN NOT NULL DEFAULT FALSE,
    status TEXT NOT NULL DEFAULT 'new', -- new, processed, skipped, failed
    status_detail TEXT,
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Índices optimizados para el estado y fecha de recepción
CREATE INDEX idx_emails_status ON emails(status);
CREATE INDEX idx_emails_received_at ON emails(received_timestamptz);

-- 2. Tabla para almacenar los archivos adjuntos en binario (BYTEA)
CREATE TABLE email_attachments (
    id BIGSERIAL PRIMARY KEY,
    email_id BIGINT NOT NULL REFERENCES emails(id) ON DELETE CASCADE,
    filename TEXT NOT NULL,
    content_type TEXT,
    size INTEGER,
    content_bytes BYTEA NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 3. Tabla de control para el cursor de sincronización
CREATE TABLE sync_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE dmarc_reports (
    id BIGSERIAL PRIMARY KEY,
    email_id BIGINT REFERENCES emails(id),
    org_name TEXT NOT NULL,
    org_email TEXT,
    report_id TEXT NOT NULL,
    begin_date TIMESTAMPTZ NOT NULL,
    end_date TIMESTAMPTZ NOT NULL,
    domain TEXT NOT NULL, -- dominio evaluado (cnzfe.gob.do)
    policy_p TEXT, -- none | quarantine | reject
    policy_sp TEXT,
    policy_pct INTEGER,
    raw JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (org_name, report_id)
);

CREATE TABLE dmarc_records (
    id BIGSERIAL PRIMARY KEY,
    report_id BIGINT NOT NULL REFERENCES dmarc_reports(id) ON DELETE CASCADE,
    source_ip INET NOT NULL,
    source_country TEXT,
    message_count INTEGER NOT NULL,
    disposition TEXT, -- none | quarantine | reject
    spf_result TEXT,  -- pass | fail
    dkim_result TEXT, -- pass | fail
    spf_aligned BOOLEAN,
    dkim_aligned BOOLEAN,
    dmarc_pass BOOLEAN, -- pasó DMARC en conjunto
    header_from TEXT,
    envelope_from TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_dmarc_records_report ON dmarc_records (report_id);
CREATE INDEX idx_dmarc_records_ip ON dmarc_records (source_ip);