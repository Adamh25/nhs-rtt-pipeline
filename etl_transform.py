"""
NHS RTT Pipeline — ETL Transform
Cleans raw data, computes NHS KPIs, writes processed outputs
"""

import pandas as pd
import numpy as np
import os, json

RAW  = "/home/claude/nhs-rtt-pipeline/data/raw"
PROC = "/home/claude/nhs-rtt-pipeline/data/processed"
os.makedirs(PROC, exist_ok=True)


# ── Load ──────────────────────────────────────────────────────────────────────

rtt  = pd.read_csv(f"{RAW}/rtt_pathways.csv",  parse_dates=["referral_date"])
ae   = pd.read_csv(f"{RAW}/ae_attendances.csv", parse_dates=["arrival_date"])
beds = pd.read_csv(f"{RAW}/bed_occupancy.csv")

print(f"Loaded: {len(rtt)} RTT rows, {len(ae)} A&E rows, {len(beds)} bed rows")


# ── RTT KPIs ──────────────────────────────────────────────────────────────────

# 1. Monthly RTT summary by trust
rtt_monthly = (
    rtt.groupby(["year_month", "trust_code", "trust_name", "region", "specialty_name"])
    .agg(
        total_pathways     = ("pathway_id", "count"),
        breaches           = ("breached_18wk", "sum"),
        avg_wait_days      = ("wait_days", "mean"),
        median_wait_days   = ("wait_days", "median"),
        pct_within_18wk    = ("breached_18wk", lambda x: round((1 - x.mean()) * 100, 1)),
        completed          = ("status", lambda x: (x == "Completed").sum()),
        still_waiting      = ("status", lambda x: (x == "Still waiting").sum()),
    )
    .reset_index()
)
rtt_monthly["avg_wait_days"]    = rtt_monthly["avg_wait_days"].round(1)
rtt_monthly["median_wait_days"] = rtt_monthly["median_wait_days"].round(1)
rtt_monthly.to_csv(f"{PROC}/rtt_monthly_kpis.csv", index=False)
print(f"  RTT monthly KPIs: {len(rtt_monthly)} rows")

# 2. Trust-level RTT performance (overall)
rtt_trust = (
    rtt.groupby(["trust_code", "trust_name", "region", "trust_type"])
    .agg(
        total_pathways   = ("pathway_id", "count"),
        breach_count     = ("breached_18wk", "sum"),
        pct_within_18wk  = ("breached_18wk", lambda x: round((1 - x.mean()) * 100, 1)),
        avg_wait_days    = ("wait_days", "mean"),
        median_wait_days = ("wait_days", "median"),
        max_wait_days    = ("wait_days", "max"),
    )
    .reset_index()
)
for col in ["avg_wait_days", "median_wait_days"]:
    rtt_trust[col] = rtt_trust[col].round(1)
rtt_trust["nhs_target_met"] = rtt_trust["pct_within_18wk"] >= 92.0  # NHS 92% standard
rtt_trust.to_csv(f"{PROC}/rtt_trust_performance.csv", index=False)
print(f"  RTT trust performance: {len(rtt_trust)} rows")

# 3. Specialty waitlist pressure
rtt_specialty = (
    rtt.groupby(["specialty_name", "specialty_code"])
    .agg(
        total            = ("pathway_id", "count"),
        still_waiting    = ("status", lambda x: (x == "Still waiting").sum()),
        breach_count     = ("breached_18wk", "sum"),
        pct_within_18wk  = ("breached_18wk", lambda x: round((1 - x.mean()) * 100, 1)),
        avg_wait_days    = ("wait_days", "mean"),
    )
    .reset_index()
)
rtt_specialty["waitlist_pressure"] = (
    rtt_specialty["still_waiting"] / rtt_specialty["total"] * 100
).round(1)
rtt_specialty["avg_wait_days"] = rtt_specialty["avg_wait_days"].round(1)
rtt_specialty.sort_values("avg_wait_days", ascending=False, inplace=True)
rtt_specialty.to_csv(f"{PROC}/rtt_specialty_pressure.csv", index=False)

# 4. ICD-10 diagnosis grouping
rtt_icd = (
    rtt.groupby(["icd10_code", "specialty_name"])
    .agg(count=("pathway_id","count"), avg_wait=("wait_days","mean"), breaches=("breached_18wk","sum"))
    .reset_index()
)
rtt_icd["avg_wait"] = rtt_icd["avg_wait"].round(1)
rtt_icd.to_csv(f"{PROC}/rtt_icd10_breakdown.csv", index=False)


# ── A&E KPIs ─────────────────────────────────────────────────────────────────

# 5. Monthly A&E summary
ae_monthly = (
    ae.groupby(["year_month", "trust_code", "trust_name", "region"])
    .agg(
        total_attendances  = ("ae_attendance_id", "count"),
        breaches_4hr       = ("breached_4hr", "sum"),
        pct_within_4hr     = ("breached_4hr", lambda x: round((1 - x.mean()) * 100, 1)),
        admissions         = ("admitted", "sum"),
        admission_rate_pct = ("admitted", lambda x: round(x.mean() * 100, 1)),
        avg_time_mins      = ("time_in_dept_mins", "mean"),
    )
    .reset_index()
)
ae_monthly["avg_time_mins"] = ae_monthly["avg_time_mins"].round(0).astype(int)
ae_monthly["nhs_4hr_target_met"] = ae_monthly["pct_within_4hr"] >= 76.0  # current NHS standard
ae_monthly.to_csv(f"{PROC}/ae_monthly_kpis.csv", index=False)
print(f"  A&E monthly KPIs: {len(ae_monthly)} rows")

# 6. A&E by arrival mode and disposal
ae_disposal = (
    ae.groupby(["disposal", "arrival_mode"])
    .agg(count=("ae_attendance_id","count"), avg_time=("time_in_dept_mins","mean"))
    .reset_index()
)
ae_disposal["avg_time"] = ae_disposal["avg_time"].round(0).astype(int)
ae_disposal.to_csv(f"{PROC}/ae_disposal_breakdown.csv", index=False)

# 7. Hourly demand pattern
ae_hourly = (
    ae.groupby("arrival_hour")
    .agg(
        total=("ae_attendance_id","count"),
        breach_rate=("breached_4hr","mean"),
        avg_time=("time_in_dept_mins","mean"),
    )
    .reset_index()
)
ae_hourly["breach_rate"] = (ae_hourly["breach_rate"] * 100).round(1)
ae_hourly["avg_time"]    = ae_hourly["avg_time"].round(0).astype(int)
ae_hourly.to_csv(f"{PROC}/ae_hourly_demand.csv", index=False)


# ── Bed Occupancy KPIs ────────────────────────────────────────────────────────

beds["high_occupancy_flag"] = (beds["occupancy_pct"] >= 95).astype(int)
beds.to_csv(f"{PROC}/bed_occupancy_processed.csv", index=False)

# DTOC summary
dtoc = (
    beds.groupby(["trust_code", "trust_name", "region"])
    .agg(
        avg_occupancy = ("occupancy_pct", "mean"),
        avg_dtoc_days = ("dtoc_days", "mean"),
        high_occ_months = ("high_occupancy_flag", "sum"),
    )
    .reset_index()
)
for col in ["avg_occupancy", "avg_dtoc_days"]:
    dtoc[col] = dtoc[col].round(1)
dtoc.to_csv(f"{PROC}/dtoc_summary.csv", index=False)
print(f"  Bed/DTOC summary: {len(dtoc)} rows")


# ── Summary Stats (for README / dashboard cards) ─────────────────────────────

summary = {
    "total_rtt_pathways":       int(len(rtt)),
    "total_ae_attendances":     int(len(ae)),
    "overall_rtt_breach_pct":   round(rtt["breached_18wk"].mean() * 100, 1),
    "overall_ae_breach_pct":    round(ae["breached_4hr"].mean() * 100, 1),
    "trusts_meeting_rtt_92":    int(rtt_trust["nhs_target_met"].sum()),
    "avg_wait_days_overall":    round(rtt["wait_days"].mean(), 1),
    "highest_wait_specialty":   rtt_specialty.iloc[0]["specialty_name"],
    "highest_wait_days":        float(rtt_specialty.iloc[0]["avg_wait_days"]),
    "trusts_analysed":          int(rtt["trust_code"].nunique()),
    "date_range":               "2023-01 to 2024-12",
}
with open(f"{PROC}/summary_stats.json", "w") as f:
    json.dump(summary, f, indent=2)

print(f"\n── Summary ──────────────────────────────────")
for k, v in summary.items():
    print(f"  {k}: {v}")
print("\nETL complete. Processed files written to data/processed/")
