from dataclasses import dataclass
from typing import Optional
import sqlite3

@dataclass
class AllPatient:
    patient_id: str
    cohort_type: str
    age_at_dx: Optional[int] = None
    sex: Optional[str] = None
    histology: Optional[str] = None
    tnm_at_dx: Optional[str] = None
    smoking_status: Optional[str] = None
    dx_year: Optional[int] = None
    last_followup: Optional[str] = None
    death_date: Optional[str] = None
    death_cause: Optional[str] = None

def save(conn: sqlite3.Connection, p: AllPatient):
    cur = conn.cursor()
    # Ровно 11 колонок, как в схеме
    cur.execute("""INSERT OR REPLACE INTO all_patients VALUES (?,?,?,?,?,?,?,?,?,?,?)""", (
        p.patient_id, p.cohort_type, p.age_at_dx, p.sex, p.histology,
        p.tnm_at_dx, p.smoking_status, p.dx_year, p.last_followup,
        p.death_date, p.death_cause
    ))
    conn.commit()
