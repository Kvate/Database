from dataclasses import dataclass
from typing import Optional
import sqlite3

@dataclass
class MolecularProfile:
    patient_id: str
    ros1_positive: int = 0
    alk_positive: int = 0
    egfr_positive: int = 0
    fusion_partner: Optional[str] = None
    ngs_report_date: Optional[str] = None
    raw_mgi: Optional[str] = None

def save(conn: sqlite3.Connection, m: MolecularProfile):
    cur = conn.cursor()
    # Теперь 7 параметров вместо 5
    cur.execute("""
        INSERT INTO molecular_profile (
            patient_id, ros1_positive, alk_positive, egfr_positive,
            fusion_partner, ngs_report_date, raw_mgi
        ) VALUES (?,?,?,?,?,?,?)
    """, (
        m.patient_id, m.ros1_positive, m.alk_positive, m.egfr_positive,
        m.fusion_partner, m.ngs_report_date, m.raw_mgi
    ))
    conn.commit()
