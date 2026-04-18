#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Web interface for NSCLC hybrid database.
Updated: Gene names in table titles are now capitalized (ALK, ROS1, EGFR, WT).
"""
import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent))
from schemas import ALL_SCHEMAS

DB_PATH = Path("data/nsclc_cohorts.db")

def get_conn():
    if not DB_PATH.exists():
        init_db()
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    Path("data").mkdir(exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    for sql in ALL_SCHEMAS: conn.executescript(sql)
    conn.close()

# ================= HELPER FOR TABLE TITLES =================
def get_table_title(tbl):
    """Returns formatted table title with Uppercase Genes names"""
    if tbl == "alk_fusion": return "ALK Fusion"
    if tbl == "ros1_fusion": return "ROS1 Fusion"
    if tbl == "egfr_mutation": return "EGFR Mutation"
    if tbl == "wt": return "WT"
    return tbl.replace("_", " ").title()

# ================= DASHBOARD =================
def render_dashboard(cohort_filter):
    st.subheader("Cohort Overview")
    where = f"WHERE cohort_type = '{cohort_filter}'" if cohort_filter != "All" else ""
    
    conn = get_conn()
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Patients", pd.read_sql(f"SELECT COUNT(*) FROM all_patients {where}", conn).iloc[0,0])
    col2.metric("Molecular Profiles", pd.read_sql(f"SELECT COUNT(*) FROM all_patients ap JOIN treatments t ON ap.patient_id=t.patient_id {where}", conn).iloc[0,0])
    col3.metric("Avg Age", f"{pd.read_sql(f'SELECT ROUND(AVG(age_at_dx),1) FROM all_patients {where}', conn).iloc[0,0]} yrs")
    
    st.divider()
    
    col_a, col_b = st.columns(2)
    with col_a:
        df_demo = pd.read_sql(f"SELECT sex, COUNT(*) as n FROM all_patients {where} GROUP BY sex", conn)
        if not df_demo.empty:
            fig = px.pie(df_demo, values='n', names='sex', title="Sex Distribution")
            st.plotly_chart(fig, use_container_width=True)
    
    with col_b:
        hist_q = f"SELECT histology, COUNT(*) as n FROM all_patients {where} {'AND' if where else 'WHERE'} histology IS NOT NULL GROUP BY histology ORDER BY n DESC LIMIT 8"
        df_hist = pd.read_sql(hist_q, conn)
        if not df_hist.empty:
            fig = px.bar(df_hist, x='histology', y='n', title="Top Histology Types")
            st.plotly_chart(fig, use_container_width=True)
            
    st.divider()
    
    st.subheader("Therapy Duration (Days)")
    dur_q = f"SELECT drug_name, duration_days FROM treatments t JOIN all_patients p ON t.patient_id=p.patient_id {where} {'AND' if where else 'WHERE'} t.drug_name IS NOT NULL"
    df_treat = pd.read_sql(dur_q, conn)
    if not df_treat.empty:
        fig = px.box(df_treat.dropna(subset=['duration_days']), x='drug_name', y='duration_days', title="Duration by Drug")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No treatment data.")
        
    st.divider()
    st.subheader("Quick Analytics")
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("Response Rate by Drug"):
            q = f"SELECT drug_name, COUNT(*) as total, ROUND(AVG(duration_days),0) as avg_days FROM treatments t JOIN all_patients p ON t.patient_id=p.patient_id {where} {'AND' if where else 'WHERE'} response IS NOT NULL GROUP BY drug_name ORDER BY avg_days DESC"
            st.dataframe(pd.read_sql(q, conn), use_container_width=True)
    with col2:
        if st.button("Age Distribution"):
            q = f"SELECT age_at_dx FROM all_patients {where} {'AND' if where else 'WHERE'} age_at_dx IS NOT NULL"
            df = pd.read_sql(q, conn)
            if not df.empty:
                st.plotly_chart(px.histogram(df, x='age_at_dx', nbins=20, title="Age at Diagnosis"), use_container_width=True)
    with col3:
        if st.button("Cohort Comparison"):
            st.dataframe(pd.read_sql("SELECT cohort_type, COUNT(*) as n, ROUND(AVG(age_at_dx),1) as avg_age FROM all_patients GROUP BY cohort_type", conn), use_container_width=True)
    conn.close()

# ================= CRUD =================
def render_crud(cohort_filter):
    st.subheader("Data Management")

    # 1. MANUAL ADD FORM
    with st.expander(" Add New Patient Manually", expanded=True):
        with st.form("add_patient_form"):
            c1, c2 = st.columns(2)
            with c1:
                pid = st.text_input("Patient ID (Required)", key="new_pid")
                cohort = st.selectbox("Cohort", ["ROS1", "ALK", "EGFR", "WT"], key="new_cohort")
                age = st.number_input("Age at Dx", min_value=0, value=None, key="new_age")
                sex = st.selectbox("Sex", ["", "М", "Ж", "Other"], key="new_sex")
            with c2:
                hist = st.text_input("Histology", key="new_hist")
                tnm = st.text_input("TNM", key="new_tnm")
                smoking = st.text_input("Smoking Status", key="new_smoking")
                dx_year = st.number_input("Diagnosis Year", min_value=1900, max_value=2030, value=None, key="new_dx_year")
            
            c3, c4 = st.columns(2)
            with c3:
                death_date = st.date_input("Death Date", value=None, key="new_death_date")
            with c4:
                last_followup = st.date_input("Last Followup", value=None, key="new_last_followup")
            
            death_cause = st.text_input("Death Cause", key="new_death_cause")
            submitted = st.form_submit_button("Add to Database")
            
            if submitted:
                if pid:
                    conn = get_conn()
                    try:
                        conn.execute("""
                            INSERT OR REPLACE INTO all_patients (
                                patient_id, cohort_type, age_at_dx, sex, histology, 
                                tnm_at_dx, smoking_status, dx_year, last_followup, death_date, death_cause
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            pid, cohort, age if age else None, sex if sex else None, hist if hist else None,
                            tnm if tnm else None, smoking if smoking else None, dx_year if dx_year else None,
                            last_followup.isoformat() if last_followup else None,
                            death_date.isoformat() if death_date else None,
                            death_cause if death_cause else None
                        ))
                        conn.commit()
                        st.success(f"Patient {pid} added!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Error: {e}")
                    finally:
                        conn.close()
                else:
                    st.error("Patient ID is required")

    # 2. SORTABLE & EDITABLE TABLES
    st.markdown("---")
    st.markdown("### 📊 Data Tables (Click headers to sort, edit cells to change)")

    tables = {
        "all_patients": ["patient_id", "cohort_type", "age_at_dx", "sex", "histology", "tnm_at_dx", "smoking_status", "dx_year", "last_followup", "death_date", "death_cause"],
        "alk_fusion": ["patient_id", "fusion_partner", "ngs_report_date", "raw_mgi"],
        "ros1_fusion": ["patient_id", "fusion_partner", "ngs_report_date", "raw_mgi"],
        "egfr_mutation": ["patient_id", "mutation_type", "ngs_report_date", "raw_mgi"],
        "wt": ["patient_id", "notes", "ngs_report_date", "raw_mgi"],
        "treatments": ["treatment_id", "patient_id", "line_number", "drug_name", "duration_days", "response", "reason_stop"],
        "outcomes": ["outcome_id", "patient_id", "pfs_months", "os_months", "censoring_pfs", "censoring_os", "notes"]
    }
    
    for tbl, cols in tables.items():
        # ИСПОЛЬЗУЕМ get_table_title для красивого отображения
        with st.expander(f"Table: {get_table_title(tbl)}"):
            where = f"WHERE patient_id IN (SELECT patient_id FROM all_patients WHERE cohort_type='{cohort_filter}')" if cohort_filter != "All" and tbl != "all_patients" else (f"WHERE cohort_type='{cohort_filter}'" if cohort_filter != "All" and tbl == "all_patients" else "")
            df = pd.read_sql(f"SELECT * FROM {tbl} {where}", get_conn())
            
            # st.data_editor allows sorting by clicking headers and editing cells
            edited = st.data_editor(df, num_rows="dynamic", use_container_width=True, hide_index=True, key=f"ed_{tbl}")
            
            if st.button(f"Save Changes to {get_table_title(tbl)}", key=f"save_{tbl}"):
                conn = get_conn()
                edited.to_sql(tbl, conn, if_exists="replace", index=False)
                conn.close()
                st.success("Saved")
                st.rerun()

# ================= IMPORT/EXPORT =================
def render_io():
    st.subheader("Data Import & Export")
    tab_imp, tab_exp = st.tabs(["Import", "Export"])
    with tab_imp:
        st.info("For cohort processing, run: python3 main.py your_file.xlsx")
        st.text_input("Or paste patient data manually", key="manual_input")
        if st.button("Add from clipboard"):
            st.success("Use main.py for bulk import")
    with tab_exp:
        st.write("Export tables")
        cols = st.columns(4)
        tables = ["all_patients", "alk_fusion", "ros1_fusion", "egfr_mutation", "wt", "treatments", "outcomes"]
        for i, tbl in enumerate(tables):
            with cols[i % 4]:
                if st.button(f"Export {get_table_title(tbl)}"): # ИСПОЛЬЗУЕМ get_table_title здесь тоже
                    df = pd.read_sql(f"SELECT * FROM {tbl}", get_conn())
                    csv = df.to_csv(index=False).encode('utf-8')
                    st.download_button(f"Download {tbl}", csv, f"{tbl}.csv", "text/csv")

# ================= MAIN =================
def main():
    st.set_page_config(page_title="NSCLC Cohort DB", layout="wide")
    st.title("NSCLC Multi-Cohort Database")
    
    sidebar = st.sidebar
    cohort_filter = sidebar.selectbox("Active Cohort", ["All", "ROS1", "ALK", "EGFR", "WT"])
    page = sidebar.radio("Module", ["Dashboard", "Data Management", "Import / Export", "SQL Runner"])
    
    if page == "Dashboard": render_dashboard(cohort_filter)
    elif page == "Data Management": render_crud(cohort_filter)
    elif page == "Import / Export": render_io()
    elif page == "SQL Runner":
        st.subheader("Custom SQL Query")
        st.info("Available tables: all_patients, treatments, outcomes, alk_fusion, ros1_fusion, egfr_mutation, wt")
        query = st.text_area("Enter SELECT query", height=100)
        if st.button("Execute"):
            if any(k in query.upper() for k in ["DROP", "DELETE", "INSERT", "UPDATE"]):
                st.error("Write operations disabled")
            else:
                conn = get_conn()
                st.dataframe(pd.read_sql(query, conn), use_container_width=True)
                conn.close()

if __name__ == "__main__":
    init_db()
    main()
