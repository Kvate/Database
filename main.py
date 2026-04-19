#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETL pipeline: Excel -> nsclc_cohorts.db
Hybrid structure, duration_days, strict column matching.
"""
import sys, sqlite3, pandas as pd, re, logging, numpy as np
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parent))
from schemas import ALL_SCHEMAS
from models import AllPatient, ALKFusion, ROS1Fusion, EGFRMutation, WT
from models import Treatment, Outcome
from models import save_patient, save_alk, save_ros1, save_egfr, save_wt, save_treatment, save_outcome

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

def parse_date(val):
    if pd.isna(val) or str(val).strip() in ("", "nan", "на", "нет"): return None
    try:
        txt = str(val).strip().replace(".", "-").replace("/", "-")
        if len(txt) == 7 and txt.count("-") == 1:
            p = txt.split("-"); txt = f"{p[1]}-{p[0].zfill(2)}-01" if len(p[0])==2 else f"{p[0]}-06-01"
        elif len(txt) == 4 and txt.isdigit(): txt = f"{txt}-01-01"
        return datetime.strptime(txt, "%Y-%m-%d").date().isoformat()
    except: return None

def extract_sample_id(raw):
    if pd.isna(raw): return None
    m = re.search(r'[A-Za-zА-Яа-яёЁ]+\d+', str(raw))
    return m.group(0) if m else str(raw).strip()

def find_col(df, keywords, exclude=[]):
    for col in df.columns:
        cl = col.lower()
        if any(k in cl for k in keywords) and not any(ex in cl for ex in exclude):
            return col
    return None

def get_val(row, col_name):
    if col_name is None: return None
    val = row.get(col_name)
    if isinstance(val, pd.Series): val = val.dropna().iloc[0] if not val.dropna().empty else None
    if pd.isna(val): return None
    return str(val).strip().replace('\n', ' ').replace('\r', ' ')

def normalize_text(val):
    if pd.isna(val) or str(val).strip() in ("", "nan", "на", "нет", "none", "н/д"): return None
    txt = str(val).strip()
    if txt.isupper() and len(txt) <= 4: return txt
    return txt.title()

def detect_cohort(filepath):
    name = str(filepath).upper()
    if "ALK" in name: return "ALK"
    if "EGFR" in name: return "EGFR"
    if "WT" in name or "WILD" in name: return "WT"
    return "ROS1"

def run_etl(excel_path: str, db_path: str):
    if not Path(excel_path).exists():
        logging.error(f"File not found: {excel_path}"); return

    raw_df = pd.read_excel(excel_path, header=None, dtype=str)
    header_idx = 0
    for i in range(min(15, len(raw_df))):
        row_text = " ".join(raw_df.iloc[i].dropna().astype(str)).lower()
        if sum(w in row_text for w in ["sample", "мги", "возраст", "вариант"]) >= 2:
            header_idx = i; break

    df = raw_df.iloc[header_idx+1:].copy()
    df.columns = [str(c).strip().replace('\n', ' ').replace('"', '') for c in raw_df.iloc[header_idx]]
    df.drop(columns=['ФИО', 'Полное ФИО пациента'], errors='ignore', inplace=True)
    
    cohort_type = detect_cohort(excel_path)
    logging.info(f"Detected cohort: {cohort_type}")

    date_excludes = ['дата', 'date', 'время', 'time', 'рожд', 'followup', 'death', 'смерт']
    cols = {
        "sample": find_col(df, ["sample", "samples", "образец"]),
        "mgi": find_col(df, ["результат мги", "вариант alk", "вариант ros1", "мутация egfr", "результат"]),
        "date_report": find_col(df, ["дата заключения", "дата получения", "дата нгс"]),
        "sex": find_col(df, ["пол"], exclude=date_excludes),
        "age": find_col(df, ["возраст на момент"]),
        "histology": find_col(df, ["гистология", "результат гистологии"]),
        "tnm": find_col(df, ["tnm на момент", "стадия"]),
        "smoking": find_col(df, ["статус курения", "курение кратко", "анамнез курения"]),
        "dx_year": find_col(df, ["в каком году диагностировали", "год диагноза"]),
        "last_followup": find_col(df, ["дата последней актуальной"]),
        "death_date": find_col(df, ["летальный исход", "дата смерти"]),
        "death_cause": find_col(df, ["если летальный исход не вызван", "причина смерти"])
    }
    logging.info(f"Found columns: { {k:v for k,v in cols.items() if v} }")

    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    for sql in ALL_SCHEMAS: conn.executescript(sql)

    success, failed = 0, 0
    for idx, row in df.iterrows():
        try:
            pid = extract_sample_id(get_val(row, cols["sample"]))
            if not pid: failed += 1; continue

            age = int(get_val(row, cols["age"])) if get_val(row, cols["age"]) and str(get_val(row, cols["age"])).isdigit() else None
            dx_y = int(get_val(row, cols["dx_year"])) if get_val(row, cols["dx_year"]) and str(get_val(row, cols["dx_year"])).isdigit() else None

            # ИСПРАВЛЕНО: удалён raw_sample, ровно 11 аргументов
            save_patient(conn, AllPatient(
                patient_id=pid, cohort_type=cohort_type,
                age_at_dx=age, sex=normalize_text(get_val(row, cols["sex"])),
                histology=normalize_text(get_val(row, cols["histology"])), tnm_at_dx=normalize_text(get_val(row, cols["tnm"])),
                smoking_status=normalize_text(get_val(row, cols["smoking"])), dx_year=dx_y,
                last_followup=parse_date(get_val(row, cols["last_followup"])),
                death_date=parse_date(get_val(row, cols["death_date"])),
                death_cause=normalize_text(get_val(row, cols["death_cause"]))
            ))

            mgi_val = get_val(row, cols["mgi"])
            if cohort_type == "ALK":
                partner = re.search(r'([A-Z0-9]+)\s*[-–—]\s*ALK', str(mgi_val).upper())
                save_alk(conn, ALKFusion(patient_id=pid, fusion_partner=partner.group(1) if partner else None,
                                         ngs_report_date=parse_date(get_val(row, cols["date_report"])), raw_mgi=mgi_val))
            elif cohort_type == "ROS1":
                partner = re.search(r'([A-Z0-9]+)\s*[-–—]\s*ROS1', str(mgi_val).upper())
                save_ros1(conn, ROS1Fusion(patient_id=pid, fusion_partner=partner.group(1) if partner else None,
                                           ngs_report_date=parse_date(get_val(row, cols["date_report"])), raw_mgi=mgi_val))
            elif cohort_type == "EGFR":
                mut = re.search(r'(Ex\d+del|L\d+R|T\d+M|[^-\s]+)', str(mgi_val).upper())
                save_egfr(conn, EGFRMutation(patient_id=pid, mutation_type=mut.group(1) if mut else None,
                                             ngs_report_date=parse_date(get_val(row, cols["date_report"])), raw_mgi=mgi_val))
            else:
                save_wt(conn, WT(patient_id=pid, notes=None, ngs_report_date=parse_date(get_val(row, cols["date_report"])), raw_mgi=mgi_val))

            line_cols = {}
            for c in df.columns:
                m = re.search(r"(\d)\s*линия", c, re.IGNORECASE)
                if m:
                    ln = int(m.group(1))
                    if ln not in line_cols: line_cols[ln] = {}
                    cl = c.lower()
                    if "препарат" in cl: line_cols[ln]["drug"] = c
                    elif "начало" in cl: line_cols[ln]["start"] = c
                    elif "конец" in cl: line_cols[ln]["end"] = c
                    elif "длительность" in cl: line_cols[ln]["dur"] = c
                    elif "эффект" in cl: line_cols[ln]["resp"] = c
                    elif "причина" in cl: line_cols[ln]["stop"] = c

            last_mapping = {}
            for ln, mapping in sorted(line_cols.items()):
                last_mapping = mapping
                drug = normalize_text(get_val(row, mapping.get("drug")))
                dur = get_val(row, mapping.get("dur"))
                dur_val = float(str(dur).replace(",", ".")) if dur and re.match(r"[\d,\.]+", str(dur)) else None
                save_treatment(conn, Treatment(
                    patient_id=pid, line_number=ln, drug_name=drug,
                    start_date=parse_date(get_val(row, mapping.get("start"))),
                    end_date=parse_date(get_val(row, mapping.get("end"))),
                    duration_days=dur_val,
                    response=normalize_text(get_val(row, mapping.get("resp"))),
                    reason_stop=normalize_text(get_val(row, mapping.get("stop")))
                ))

            first_dur = conn.execute("SELECT duration_days FROM treatments WHERE patient_id=? ORDER BY line_number LIMIT 1", (pid,)).fetchone()
            pfs = first_dur[0] if first_dur else None
            os_val = None
            if dx_y:
                ref_date = parse_date(get_val(row, cols["death_date"])) or parse_date(get_val(row, cols["last_followup"]))
                if ref_date: os_val = (datetime.strptime(ref_date, "%Y-%m-%d").year - dx_y) * 12 + 6

            resp_val = get_val(row, last_mapping.get("resp"))
            is_progression = resp_val and "прогресс" in str(resp_val).lower()
            save_outcome(conn, Outcome(
                patient_id=pid, pfs_months=pfs, os_months=os_val,
                censoring_pfs=not is_progression,
                censoring_os=bool(get_val(row, cols["death_date"]) is None)
            ))
            success += 1
        except Exception as e:
            failed += 1
            logging.warning(f"Row {idx} skipped: {e}")

    conn.commit()
    conn.close()
    logging.info(f"Done. Success: {success}, Failed: {failed}")
    logging.info(f"Database updated: {Path(db_path).resolve()}")

if __name__ == "__main__":
    BASE_DIR = Path(__file__).resolve().parent
    DB_FILE = BASE_DIR / "data" / "nsclc_cohorts.db"
    Path("data").mkdir(exist_ok=True)
    excel = sys.argv[1] if len(sys.argv) > 1 else None
    if not excel:
        logging.error("Usage: python3 main.py <excel_file.xlsx>")
        sys.exit(1)
    run_etl(excel, str(DB_FILE))
