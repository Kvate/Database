from dataclasses import dataclass
from typing import Optional
import sqlite3

@dataclass
class EGFRMutation:
    patient_id: str
    mutation_type: Optional[str] = None
    ngs_report_date: Optional[str] = None
    raw_mgi: Optional[str] = None

def save(conn: sqlite3.Connection, m: EGFRMutation):
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO egfr_mutation VALUES (?,?,?,?)", (m.patient_id, m.mutation_type, m.ngs_report_date, m.raw_mgi))
    conn.commit()
