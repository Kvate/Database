from dataclasses import dataclass
from typing import Optional
import sqlite3

@dataclass
class ALKFusion:
    patient_id: str
    fusion_partner: Optional[str] = None
    ngs_report_date: Optional[str] = None
    raw_mgi: Optional[str] = None

def save(conn: sqlite3.Connection, m: ALKFusion):
    cur = conn.cursor()
    cur.execute("INSERT OR REPLACE INTO alk_fusion VALUES (?,?,?,?)", (m.patient_id, m.fusion_partner, m.ngs_report_date, m.raw_mgi))
    conn.commit()
