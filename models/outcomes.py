from dataclasses import dataclass
from typing import Optional
import sqlite3

@dataclass
class Outcome:
    patient_id: str
    pfs_months: Optional[float] = None
    os_months: Optional[float] = None
    censoring_pfs: bool = True
    censoring_os: bool = True
    notes: Optional[str] = None

def save(conn: sqlite3.Connection, o: Outcome):
    cur = conn.cursor()
    cur.execute("""INSERT INTO outcomes (patient_id, pfs_months, os_months, censoring_pfs, censoring_os, notes)
                   VALUES (?,?,?,?,?,?)""", (o.patient_id, o.pfs_months, o.os_months, int(o.censoring_pfs), int(o.censoring_os), o.notes))
    conn.commit()
