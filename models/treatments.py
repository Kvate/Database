from dataclasses import dataclass
from typing import Optional
import sqlite3

@dataclass
class Treatment:
    patient_id: str
    line_number: int
    drug_name: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    duration_days: Optional[float] = None
    response: Optional[str] = None
    reason_stop: Optional[str] = None

def save(conn: sqlite3.Connection, t: Treatment):
    cur = conn.cursor()
    cur.execute("""INSERT INTO treatments (patient_id, line_number, drug_name, start_date, end_date, duration_days, response, reason_stop)
                   VALUES (?,?,?,?,?,?,?,?)""", (t.patient_id, t.line_number, t.drug_name, t.start_date, t.end_date, t.duration_days, t.response, t.reason_stop))
    conn.commit()
