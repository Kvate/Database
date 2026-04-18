from dataclasses import dataclass
from typing import Optional
import sqlite3

@dataclass
class WT:
    patient_id: str
    notes: Optional[str] = None
    ngs_report_date: Optional[str] = None
    raw_mgi: Optional[str] = None

def save(conn: sqlite3.Connection, m: WT):
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO wt VALUES (?,?,?,?)", (m.patient_id, m.notes, m.ngs_report_date, m.raw_mgi))
    conn.commit()
