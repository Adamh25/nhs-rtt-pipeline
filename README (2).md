# NHS RTT Pathway & Operational Performance Analytics

End-to-end data pipeline and analytics dashboard modelled on NHS England's Referral-to-Treatment (RTT) and A&E operational reporting structure. Built to demonstrate practical knowledge of NHS data standards, KPI frameworks, and healthcare BI.

## Project Overview

The NHS 18-week RTT standard requires 92% of patients to receive treatment within 18 weeks of referral. A&E departments are measured against a 4-hour wait standard. This project replicates the analytical workflow an NHS Business Intelligence or Data Analyst team would perform to monitor elective recovery and operational pressure across a cohort of Trusts.

**Data covers:** 10 NHS Trusts · 15,000 RTT pathways · 20,000 A&E attendances · 24 months (2023–2024)  
**Note:** All data is synthetic, generated to mirror real NHS SUS/HES structure and statistics.

---

## Key Findings

| Metric | Value | NHS Standard |
|---|---|---|
| RTT 18-week breach rate | 32.4% | < 8% (92% standard) |
| Average wait (days) | 113.6 | ≤ 126 days |
| A&E 4-hour breach rate | 26.2% | < 24% (76% standard) |
| Trusts meeting 92% RTT standard | 0 / 10 | 10 / 10 |
| Highest-pressure specialty | Cardiothoracic Surgery | 154 days avg wait |

Notable: breach rate rose sharply from ~27% (2023) to ~38% (2024), consistent with real NHS elective recovery trends post-pandemic.

---

## NHS Data Standards Applied

- **ICD-10** clinical coding for diagnosis grouping (chapters G, H, I, J, K, L, M, N, R)
- **Treatment Function Codes (TFC)** for specialty mapping (C_100–C_420 range)
- **ODS Trust codes** (RAX, RJ1, RRV, RQ6, etc.)
- **NHS RTT pathway rules** — clock start, clock stop, 18-week breach definition
- **SUS/HES star schema** — fact/dimension model matching NHS Digital data architecture
- **A&E SITREP structure** — 4-hour standard, disposal codes, admission rates
- **DTOC reporting** — delayed transfers of care alongside bed occupancy

---

## Technical Stack

| Layer | Technology |
|---|---|
| Data generation | Python (pandas, numpy) |
| ETL / transformation | Python (pandas aggregations, KPI derivation) |
| Data model | SQL Server / Azure SQL compatible star schema |
| Analytical views | T-SQL (CTEs, window functions) |
| Dashboard | HTML/CSS/JS with Chart.js |
| Storage | CSV (production equivalent: Azure SQL / Databricks) |

---

## Repository Structure

```
nhs-rtt-pipeline/
├── scripts/
│   ├── generate_data.py        # Synthetic data generation (NHS-structured)
│   └── etl_transform.py        # KPI computation and ETL transforms
├── data/
│   ├── raw/
│   │   ├── rtt_pathways.csv        # 15,000 RTT pathway records
│   │   ├── ae_attendances.csv      # 20,000 A&E attendance records
│   │   └── bed_occupancy.csv       # Monthly bed occupancy by trust
│   └── processed/
│       ├── rtt_monthly_kpis.csv
│       ├── rtt_trust_performance.csv
│       ├── rtt_specialty_pressure.csv
│       ├── ae_monthly_kpis.csv
│       ├── dtoc_summary.csv
│       └── summary_stats.json
├── sql/
│   └── schema.sql              # Star schema DDL + analytical views
└── dashboard/
    └── index.html              # Interactive Chart.js dashboard
```

---

## KPIs Tracked

**RTT / Elective:**
- % of pathways within 18 weeks (vs 92% NHS standard)
- Average and median wait time by trust and specialty
- Still-waiting list size by specialty (waitlist pressure)
- Year-on-year breach rate change
- ICD-10 demand distribution

**A&E / Urgent Care:**
- 4-hour compliance rate (vs 76% NHS standard)
- Average time in department
- Admission rate by trust
- Disposal outcome breakdown (discharged, admitted, LWBS, etc.)

**Capacity / Flow:**
- Bed occupancy percentage by trust
- Delayed transfers of care (DTOC) — a leading indicator of discharge pressure
- Teaching vs DGH performance comparison

---

## Running Locally

```bash
# Install dependencies
pip install pandas numpy

# Generate synthetic data
python scripts/generate_data.py

# Run ETL transforms
python scripts/etl_transform.py

# View dashboard
open dashboard/index.html
```

---

## Context & Motivation

Built as a portfolio project to demonstrate domain knowledge relevant to NHS data analyst and business analyst roles. The RTT pathway structure, A&E SITREP reporting, ICD-10/OPCS coding, ODS trust identifiers, and SUS/HES dimensional model are all drawn from NHS England's published data standards and methodology.

**Author:** Adam Hussain | [GitHub](https://github.com/Adamh25) | [LinkedIn](https://linkedin.com/in/adamhussain25)
