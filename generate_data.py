"""
NHS RTT Pathway & A&E Attendance Data Generator
Generates realistic synthetic data mirroring NHS England SUS/HES structure
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

random.seed(42)
np.random.seed(42)

# ── Constants ─────────────────────────────────────────────────────────────────

TRUSTS = [
    {"code": "RJ1", "name": "Guy's and St Thomas' NHS FT",       "region": "London",       "type": "Teaching"},
    {"code": "RRV", "name": "University College London Hospitals","region": "London",       "type": "Teaching"},
    {"code": "RM2", "name": "University Hospitals of Leicester",  "region": "Midlands",     "type": "Teaching"},
    {"code": "RXW", "name": "Shrewsbury and Telford Hospital",    "region": "Midlands",     "type": "DGH"},
    {"code": "RX1", "name": "Nottingham University Hospitals",    "region": "Midlands",     "type": "Teaching"},
    {"code": "RQ6", "name": "Manchester University NHS FT",       "region": "North West",   "type": "Teaching"},
    {"code": "RBD", "name": "Dorset County Hospital",             "region": "South West",   "type": "DGH"},
    {"code": "RDE", "name": "Essex Partnership University NHS FT","region": "East of England","type":"DGH"},
    {"code": "RAX", "name": "Kingston Hospital NHS FT",           "region": "London",       "type": "DGH"},
    {"code": "RVV", "name": "East Kent Hospitals University FT",  "region": "South East",   "type": "DGH"},
]

SPECIALTIES = [
    {"code": "C_100", "name": "General Surgery",         "icd_chapter": "K",  "avg_wait_days": 82},
    {"code": "C_110", "name": "Urology",                 "icd_chapter": "N",  "avg_wait_days": 95},
    {"code": "C_120", "name": "Trauma & Orthopaedics",   "icd_chapter": "M",  "avg_wait_days": 110},
    {"code": "C_130", "name": "ENT",                     "icd_chapter": "H",  "avg_wait_days": 88},
    {"code": "C_140", "name": "Ophthalmology",           "icd_chapter": "H",  "avg_wait_days": 105},
    {"code": "C_150", "name": "Oral Surgery",            "icd_chapter": "K",  "avg_wait_days": 78},
    {"code": "C_160", "name": "Neurosurgery",            "icd_chapter": "G",  "avg_wait_days": 120},
    {"code": "C_170", "name": "Cardiothoracic Surgery",  "icd_chapter": "I",  "avg_wait_days": 130},
    {"code": "C_300", "name": "General Medicine",        "icd_chapter": "R",  "avg_wait_days": 65},
    {"code": "C_320", "name": "Cardiology",              "icd_chapter": "I",  "avg_wait_days": 95},
    {"code": "C_330", "name": "Dermatology",             "icd_chapter": "L",  "avg_wait_days": 88},
    {"code": "C_340", "name": "Respiratory Medicine",    "icd_chapter": "J",  "avg_wait_days": 72},
    {"code": "C_400", "name": "Neurology",               "icd_chapter": "G",  "avg_wait_days": 115},
    {"code": "C_410", "name": "Rheumatology",            "icd_chapter": "M",  "avg_wait_days": 100},
    {"code": "C_420", "name": "Gastroenterology",        "icd_chapter": "K",  "avg_wait_days": 90},
]

# ICD-10 codes per chapter (simplified)
ICD10_CODES = {
    "K": ["K40", "K57", "K80", "K92", "K35", "K56", "K43"],
    "N": ["N13", "N20", "N32", "N40", "N18", "N04"],
    "M": ["M16", "M17", "M47", "M51", "M06", "M79"],
    "H": ["H26", "H33", "H25", "H66", "H91", "H04"],
    "G": ["G35", "G43", "G54", "G61", "G20", "G51"],
    "I": ["I20", "I25", "I35", "I48", "I50", "I63"],
    "J": ["J18", "J44", "J45", "J06", "J22"],
    "L": ["L40", "L20", "L70", "L50", "L30"],
    "R": ["R07", "R10", "R55", "R00", "R05"],
}

PRIORITY_TYPES = ["Urgent", "Routine"]
PATHWAY_TYPES  = ["New", "Review"]
TREATMENT_STATUS = ["Completed", "Still waiting", "Removed from list"]

AE_CHIEF_COMPLAINTS = [
    "Chest pain", "Shortness of breath", "Abdominal pain", "Head injury",
    "Fall", "Stroke symptoms", "Sepsis", "Fracture", "Lacerations",
    "Allergic reaction", "Seizure", "Mental health crisis", "Overdose",
]
AE_DISPOSALS = {
    "Admitted": 0.28,
    "Discharged": 0.55,
    "Referred to GP": 0.08,
    "Left without being seen": 0.05,
    "Died in department": 0.004,
    "Transferred": 0.036,
}
AE_ARRIVAL_MODES = ["Ambulance", "Walk-in", "GP referral", "Police", "Other"]

START_DATE = datetime(2023, 1, 1)
END_DATE   = datetime(2024, 12, 31)


# ── Helpers ───────────────────────────────────────────────────────────────────

def rand_date(start, end):
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_nhs_number():
    return f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"


# ── RTT Dataset ───────────────────────────────────────────────────────────────

def generate_rtt(n=15000):
    rows = []
    for _ in range(n):
        trust      = random.choice(TRUSTS)
        specialty  = random.choice(SPECIALTIES)
        priority   = random.choices(PRIORITY_TYPES, weights=[0.15, 0.85])[0]
        referral_date = rand_date(START_DATE, END_DATE - timedelta(days=30))

        # Wait time — skewed, longer for routine, spikes in 2024
        base_wait = specialty["avg_wait_days"]
        year_factor = 1.12 if referral_date.year == 2024 else 1.0
        if trust["type"] == "DGH":
            base_wait *= 1.08
        wait_days = max(1, int(np.random.lognormal(np.log(base_wait * year_factor), 0.4)))

        treatment_date = referral_date + timedelta(days=wait_days)
        breached_18wk  = wait_days > 126  # 18 weeks = 126 days

        status = random.choices(
            TREATMENT_STATUS,
            weights=[0.65, 0.25, 0.10]
        )[0]

        icd_chapter = specialty["icd_chapter"]
        icd_code    = random.choice(ICD10_CODES.get(icd_chapter, ["R99"]))

        rows.append({
            "pathway_id":       f"RTT{random.randint(100000,999999)}",
            "nhs_number":       rand_nhs_number(),
            "trust_code":       trust["code"],
            "trust_name":       trust["name"],
            "region":           trust["region"],
            "trust_type":       trust["type"],
            "specialty_code":   specialty["code"],
            "specialty_name":   specialty["name"],
            "icd10_code":       icd_code,
            "priority":         priority,
            "pathway_type":     random.choice(PATHWAY_TYPES),
            "referral_date":    referral_date.strftime("%Y-%m-%d"),
            "treatment_date":   treatment_date.strftime("%Y-%m-%d") if status == "Completed" else None,
            "wait_days":        wait_days,
            "weeks_waited":     round(wait_days / 7, 1),
            "breached_18wk":    int(breached_18wk),
            "status":           status,
            "year_month":       referral_date.strftime("%Y-%m"),
            "referral_year":    referral_date.year,
        })

    return pd.DataFrame(rows)


# ── A&E Dataset ───────────────────────────────────────────────────────────────

def generate_ae(n=20000):
    rows = []
    disposal_choices = list(AE_DISPOSALS.keys())
    disposal_weights = list(AE_DISPOSALS.values())

    for _ in range(n):
        trust       = random.choice(TRUSTS)
        arrival_dt  = rand_date(START_DATE, END_DATE)

        # Time in department — target is 4 hours (240 min)
        base_time = 180
        if arrival_dt.hour in range(10, 22):  # peak hours
            base_time = 230
        time_in_dept = max(15, int(np.random.lognormal(np.log(base_time), 0.45)))
        breached_4hr = int(time_in_dept > 240)

        disposal = random.choices(disposal_choices, weights=disposal_weights)[0]
        admitted  = int(disposal == "Admitted")

        rows.append({
            "ae_attendance_id":  f"AE{random.randint(1000000,9999999)}",
            "trust_code":        trust["code"],
            "trust_name":        trust["name"],
            "region":            trust["region"],
            "arrival_date":      arrival_dt.strftime("%Y-%m-%d"),
            "arrival_hour":      arrival_dt.hour,
            "arrival_mode":      random.choices(AE_ARRIVAL_MODES, weights=[0.30,0.45,0.15,0.03,0.07])[0],
            "chief_complaint":   random.choice(AE_CHIEF_COMPLAINTS),
            "time_in_dept_mins": time_in_dept,
            "breached_4hr":      breached_4hr,
            "disposal":          disposal,
            "admitted":          admitted,
            "year_month":        arrival_dt.strftime("%Y-%m"),
            "arrival_year":      arrival_dt.year,
        })

    return pd.DataFrame(rows)


# ── Bed Occupancy Dataset ─────────────────────────────────────────────────────

def generate_bed_occupancy():
    rows = []
    months = pd.date_range(START_DATE, END_DATE, freq="MS")
    for trust in TRUSTS:
        for month in months:
            base_occ = 88 if trust["type"] == "Teaching" else 84
            seasonal = 3 * np.sin((month.month - 1) * np.pi / 6)  # winter peak
            occupancy = min(99.5, max(70, base_occ + seasonal + np.random.normal(0, 2)))
            total_beds = random.randint(400, 1200) if trust["type"] == "Teaching" else random.randint(150, 450)
            dtoc_days  = max(0, int(np.random.normal(12, 4)))  # delayed transfers of care
            rows.append({
                "trust_code":      trust["code"],
                "trust_name":      trust["name"],
                "region":          trust["region"],
                "trust_type":      trust["type"],
                "year_month":      month.strftime("%Y-%m"),
                "total_beds":      total_beds,
                "occupancy_pct":   round(occupancy, 1),
                "dtoc_days":       dtoc_days,
            })
    return pd.DataFrame(rows)


# ── Run ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    out = "/home/claude/nhs-rtt-pipeline/data/raw"
    os.makedirs(out, exist_ok=True)

    print("Generating RTT pathway data (15,000 rows)...")
    rtt = generate_rtt(15000)
    rtt.to_csv(f"{out}/rtt_pathways.csv", index=False)
    print(f"  → {len(rtt)} rows | Breach rate: {rtt['breached_18wk'].mean()*100:.1f}%")

    print("Generating A&E attendance data (20,000 rows)...")
    ae = generate_ae(20000)
    ae.to_csv(f"{out}/ae_attendances.csv", index=False)
    print(f"  → {len(ae)} rows | 4hr breach rate: {ae['breached_4hr'].mean()*100:.1f}%")

    print("Generating bed occupancy data...")
    beds = generate_bed_occupancy()
    beds.to_csv(f"{out}/bed_occupancy.csv", index=False)
    print(f"  → {len(beds)} rows")

    print("\nDone. Files saved to data/raw/")
