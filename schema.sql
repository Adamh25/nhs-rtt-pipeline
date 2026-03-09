-- ============================================================
-- NHS RTT PATHWAY ANALYTICS — Star Schema DDL
-- Mirrors NHS SUS/HES dimensional model
-- Compatible with: SQL Server / Azure SQL / PostgreSQL
-- ============================================================

-- ── Dimension: Date ───────────────────────────────────────────
CREATE TABLE dim_date (
    date_id         INT           PRIMARY KEY,
    full_date       DATE          NOT NULL,
    year_month      VARCHAR(7)    NOT NULL,   -- e.g. '2024-03'
    calendar_year   SMALLINT      NOT NULL,
    calendar_month  TINYINT       NOT NULL,
    month_name      VARCHAR(12)   NOT NULL,
    quarter         TINYINT       NOT NULL,
    nhs_year        VARCHAR(9)    NOT NULL,   -- e.g. '2023/24'
    is_weekend      BIT           DEFAULT 0
);

-- ── Dimension: Trust ──────────────────────────────────────────
CREATE TABLE dim_trust (
    trust_sk        INT           PRIMARY KEY IDENTITY(1,1),
    trust_code      CHAR(3)       NOT NULL UNIQUE,  -- ODS code
    trust_name      VARCHAR(100)  NOT NULL,
    region          VARCHAR(50)   NOT NULL,
    trust_type      VARCHAR(20)   NOT NULL,          -- Teaching / DGH
    icb_code        CHAR(3)       NULL,              -- Integrated Care Board
    valid_from      DATE          NOT NULL,
    valid_to        DATE          NULL,              -- SCD2 support
    is_current      BIT           DEFAULT 1
);

-- ── Dimension: Specialty (Treatment Function Code) ────────────
CREATE TABLE dim_specialty (
    specialty_sk    INT           PRIMARY KEY IDENTITY(1,1),
    specialty_code  VARCHAR(10)   NOT NULL UNIQUE,  -- TFC e.g. C_120
    specialty_name  VARCHAR(80)   NOT NULL,
    icd_chapter     CHAR(1)       NOT NULL,
    surgery_flag    BIT           DEFAULT 0,
    urgent_pathway  BIT           DEFAULT 0
);

-- ── Dimension: ICD-10 ─────────────────────────────────────────
CREATE TABLE dim_icd10 (
    icd10_sk        INT           PRIMARY KEY IDENTITY(1,1),
    icd10_code      VARCHAR(8)    NOT NULL UNIQUE,
    icd10_desc      VARCHAR(200)  NOT NULL,
    chapter_code    CHAR(1)       NOT NULL,
    chapter_desc    VARCHAR(100)  NOT NULL,
    block_range     VARCHAR(20)   NULL
);

-- ── Fact: RTT Pathways ────────────────────────────────────────
-- Grain: one row per patient RTT pathway
CREATE TABLE fact_rtt_pathway (
    pathway_sk          BIGINT        PRIMARY KEY IDENTITY(1,1),
    pathway_id          VARCHAR(12)   NOT NULL,      -- source key
    referral_date_id    INT           NOT NULL REFERENCES dim_date(date_id),
    treatment_date_id   INT           NULL     REFERENCES dim_date(date_id),
    trust_sk            INT           NOT NULL REFERENCES dim_trust(trust_sk),
    specialty_sk        INT           NOT NULL REFERENCES dim_specialty(specialty_sk),
    icd10_sk            INT           NULL     REFERENCES dim_icd10(icd10_sk),

    -- Pathway attributes
    priority_type       VARCHAR(10)   NOT NULL CHECK (priority_type IN ('Urgent','Routine')),
    pathway_type        VARCHAR(10)   NOT NULL CHECK (pathway_type IN ('New','Review')),
    status              VARCHAR(20)   NOT NULL,

    -- Measures
    wait_days           SMALLINT      NOT NULL,
    weeks_waited        DECIMAL(5,1)  NOT NULL,
    breached_18wk       BIT           NOT NULL DEFAULT 0,

    -- Audit
    load_timestamp      DATETIME2     DEFAULT GETDATE()
);

-- ── Fact: A&E Attendances ─────────────────────────────────────
-- Grain: one row per A&E attendance
CREATE TABLE fact_ae_attendance (
    ae_sk               BIGINT        PRIMARY KEY IDENTITY(1,1),
    ae_attendance_id    VARCHAR(12)   NOT NULL,
    arrival_date_id     INT           NOT NULL REFERENCES dim_date(date_id),
    trust_sk            INT           NOT NULL REFERENCES dim_trust(trust_sk),

    -- Arrival context
    arrival_hour        TINYINT       NOT NULL CHECK (arrival_hour BETWEEN 0 AND 23),
    arrival_mode        VARCHAR(20)   NOT NULL,
    chief_complaint     VARCHAR(50)   NULL,

    -- Outcomes
    time_in_dept_mins   SMALLINT      NOT NULL,
    breached_4hr        BIT           NOT NULL DEFAULT 0,
    disposal            VARCHAR(30)   NOT NULL,
    admitted            BIT           NOT NULL DEFAULT 0,

    load_timestamp      DATETIME2     DEFAULT GETDATE()
);

-- ── Fact: Bed Occupancy (monthly snapshot) ───────────────────
CREATE TABLE fact_bed_occupancy (
    occupancy_sk        INT           PRIMARY KEY IDENTITY(1,1),
    snapshot_date_id    INT           NOT NULL REFERENCES dim_date(date_id),
    trust_sk            INT           NOT NULL REFERENCES dim_trust(trust_sk),

    total_beds          SMALLINT      NOT NULL,
    occupancy_pct       DECIMAL(5,1)  NOT NULL,
    dtoc_days           SMALLINT      NOT NULL,          -- delayed transfer of care days
    high_occupancy_flag BIT           NOT NULL DEFAULT 0 -- >=95%

    CONSTRAINT uq_occ UNIQUE (snapshot_date_id, trust_sk)
);

-- ── Indexes ───────────────────────────────────────────────────
CREATE INDEX ix_rtt_trust     ON fact_rtt_pathway(trust_sk);
CREATE INDEX ix_rtt_specialty ON fact_rtt_pathway(specialty_sk);
CREATE INDEX ix_rtt_ref_date  ON fact_rtt_pathway(referral_date_id);
CREATE INDEX ix_rtt_breach    ON fact_rtt_pathway(breached_18wk);
CREATE INDEX ix_ae_trust      ON fact_ae_attendance(trust_sk);
CREATE INDEX ix_ae_date       ON fact_ae_attendance(arrival_date_id);
CREATE INDEX ix_ae_breach     ON fact_ae_attendance(breached_4hr);


-- ── Analytical Views ──────────────────────────────────────────

-- Monthly RTT KPIs
CREATE VIEW vw_rtt_monthly_kpis AS
SELECT
    dd.year_month,
    dd.nhs_year,
    dt.trust_code,
    dt.trust_name,
    dt.region,
    ds.specialty_name,
    COUNT(*)                                     AS total_pathways,
    SUM(f.breached_18wk)                         AS breach_count,
    ROUND(100.0 * (1 - AVG(CAST(f.breached_18wk AS FLOAT))), 1) AS pct_within_18wk,
    AVG(f.wait_days)                             AS avg_wait_days,
    SUM(CASE WHEN f.status = 'Still waiting' THEN 1 ELSE 0 END) AS still_waiting
FROM fact_rtt_pathway f
JOIN dim_date      dd ON f.referral_date_id = dd.date_id
JOIN dim_trust     dt ON f.trust_sk         = dt.trust_sk
JOIN dim_specialty ds ON f.specialty_sk     = ds.specialty_sk
GROUP BY dd.year_month, dd.nhs_year, dt.trust_code, dt.trust_name, dt.region, ds.specialty_name;


-- Trust RTT Performance vs 92% Standard
CREATE VIEW vw_trust_rtt_vs_standard AS
SELECT
    dt.trust_code,
    dt.trust_name,
    dt.region,
    dt.trust_type,
    COUNT(*)                                     AS total_pathways,
    SUM(f.breached_18wk)                         AS breach_count,
    ROUND(100.0 * (1 - AVG(CAST(f.breached_18wk AS FLOAT))), 1) AS pct_within_18wk,
    AVG(CAST(f.wait_days AS FLOAT))              AS avg_wait_days,
    CASE WHEN ROUND(100.0 * (1 - AVG(CAST(f.breached_18wk AS FLOAT))),1) >= 92.0
         THEN 'MET' ELSE 'MISSED' END           AS nhs_standard_status
FROM fact_rtt_pathway f
JOIN dim_trust dt ON f.trust_sk = dt.trust_sk
GROUP BY dt.trust_code, dt.trust_name, dt.region, dt.trust_type;


-- A&E 4-hour compliance
CREATE VIEW vw_ae_4hr_performance AS
SELECT
    dd.year_month,
    dt.trust_name,
    COUNT(*)                                         AS total_attendances,
    SUM(f.breached_4hr)                              AS breaches,
    ROUND(100.0 * (1 - AVG(CAST(f.breached_4hr AS FLOAT))), 1) AS pct_within_4hr,
    SUM(f.admitted)                                  AS admissions,
    ROUND(100.0 * AVG(CAST(f.admitted AS FLOAT)), 1) AS admission_rate_pct,
    CASE WHEN ROUND(100.0 * (1 - AVG(CAST(f.breached_4hr AS FLOAT))),1) >= 76.0
         THEN 'MET' ELSE 'MISSED' END                AS nhs_standard_status
FROM fact_ae_attendance f
JOIN dim_date  dd ON f.arrival_date_id = dd.date_id
JOIN dim_trust dt ON f.trust_sk        = dt.trust_sk
GROUP BY dd.year_month, dt.trust_name;
