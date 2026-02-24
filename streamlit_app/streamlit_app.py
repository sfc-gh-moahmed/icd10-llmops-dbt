"""
ICD-10 LLMOps Demo - Streamlit in Snowflake (SiS)
dbt-powered LLM evaluation framework for medical coding extraction
Replicates Agilon Production SP Logic
"""
import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
from datetime import datetime
import json

# Get Snowflake session
session = get_active_session()

# Configuration
DATABASE = "ICD10_POC"
SCHEMA = "ICD10_DEMO"  # Source data schema
DBT_SCHEMA = "ICD10_DEMO_ICD10_DEMO"  # dbt outputs to this schema
DBT_PROJECT = "ICD10_LLMOPS_PROJECT"

st.set_page_config(
    page_title="ICD-10 LLMOps Demo",
    page_icon="🏥",
    layout="wide"
)

# Initialize session state
if 'experiment_history' not in st.session_state:
    st.session_state.experiment_history = []

def run_query(sql):
    """Execute SQL and return DataFrame"""
    try:
        return session.sql(sql).to_pandas()
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame()

def run_dbt_project():
    """Execute the deployed dbt project"""
    dbt_sql = f"EXECUTE DBT PROJECT {DATABASE}.{SCHEMA}.{DBT_PROJECT}"
    try:
        result = session.sql(dbt_sql).collect()
        return True, "dbt project executed successfully", ""
    except Exception as e:
        return False, "", str(e)

# Header
st.title("ICD-10 Code Extraction - LLMOps Demo")
st.markdown("""
**dbt-powered evaluation framework for medical coding extraction using Snowflake Cortex**  
*Replicates Agilon Production Stored Procedure Logic with structured JSON output*
""")

# Sidebar
st.sidebar.header("Configuration")
st.sidebar.info(f"""
**Database:** {DATABASE}  
**Source Schema:** {SCHEMA}  
**dbt Output Schema:** {DBT_SCHEMA}  
**dbt Project:** {DBT_PROJECT}
""")

st.sidebar.markdown("---")
st.sidebar.subheader("SP Logic Features")
st.sidebar.markdown("""
- Batch grouping per patient
- System + User message prompts
- Structured JSON output schema
- Evidence-backed justifications
- Batch group summaries
""")

# Step 1: Source Data
st.header("Step 1: Source Data Overview")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Patient Clinical Events")
    if st.button("Load Source Data", key="load_source"):
        df = run_query(f"""
            SELECT AGILON_MEMBER_ID AS PATIENT_ID, BATCH_NUMBER, 
                   LEFT(BATCHED_EVENTS, 200) || '...' AS CLINICAL_PREVIEW,
                   MIN_EVENT_DATE, MAX_EVENT_DATE,
                   LENGTH(BATCHED_EVENTS) AS TEXT_LENGTH
            FROM {DATABASE}.{SCHEMA}.BATCHED_EVENTS ORDER BY AGILON_MEMBER_ID
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            st.success(f"Loaded {len(df)} patient records")

with col2:
    st.subheader("Ground Truth Labels")
    if st.button("Load Ground Truth", key="load_gt"):
        df = run_query(f"""
            SELECT AGILON_MEMBER_ID AS PATIENT_ID, EXPECTED_ICD10_CODES, NOTES
            FROM {DATABASE}.{SCHEMA}.GROUND_TRUTH ORDER BY AGILON_MEMBER_ID
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True)
            st.success(f"Loaded {len(df)} ground truth records")

st.markdown("---")

# Step 2: Run dbt Pipeline
st.header("Step 2: Run LLM Extraction Pipeline")
st.markdown("""
This executes the **dbt project** which replicates the **Agilon Production SP Logic**:
1. Groups patient batches and builds structured prompts
2. Calls **Snowflake Cortex COMPLETE** (claude-3-5-sonnet) with system + user messages
3. Parses structured JSON output with ICD-10 codes, justifications, and evidence
4. Evaluates against ground truth with **Precision, Recall, and F1 scores**
""")

if st.button("Run dbt Pipeline", type="primary", key="run_dbt"):
    with st.spinner("Running dbt pipeline... This may take 1-2 minutes for LLM processing."):
        success, stdout, stderr = run_dbt_project()
        if success:
            st.success("dbt pipeline completed successfully!")
            st.session_state.experiment_history.append({
                "timestamp": datetime.now().isoformat(),
                "status": "success"
            })
        else:
            st.error(f"dbt pipeline failed: {stderr}")

st.markdown("---")

# Step 3: View LLM Extraction Results
st.header("Step 3: LLM Extraction Results")

tab1, tab2, tab3 = st.tabs(["Summary View", "Extracted Codes Detail", "Raw JSON Response"])

with tab1:
    if st.button("Load Extraction Summary", key="load_extraction_summary"):
        df = run_query(f"""
            SELECT PATIENT_ID, 
                   BATCH_GROUP_RANGE,
                   NUM_CODES_EXTRACTED,
                   LEFT(BATCH_GROUP_SUMMARY, 150) || '...' AS SUMMARY_PREVIEW,
                   MODEL_USED, 
                   EXTRACTED_AT
            FROM {DATABASE}.{DBT_SCHEMA}.LLM_SP_RESULTS 
            ORDER BY PATIENT_ID
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True, height=400)
            
            # Show totals
            total_codes = df['NUM_CODES_EXTRACTED'].sum()
            st.info(f"**Total Codes Extracted:** {total_codes} across {len(df)} patients")
        else:
            st.warning("No results found. Run dbt pipeline first.")

with tab2:
    if st.button("Load Extracted Codes with Justifications", key="load_codes_detail"):
        df = run_query(f"""
            SELECT PATIENT_ID,
                   ICD10_CODE,
                   LEFT(JUSTIFICATION, 200) AS JUSTIFICATION,
                   EVIDENCE_COUNT,
                   BATCH_GROUP_RANGE
            FROM {DATABASE}.{DBT_SCHEMA}.LLM_EXTRACTED_CODES 
            ORDER BY PATIENT_ID, CODE_INDEX
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True, height=500)
            
            # Code distribution
            st.subheader("Code Distribution")
            code_counts = df['ICD10_CODE'].value_counts().head(10)
            st.bar_chart(code_counts)
        else:
            st.warning("No extracted codes found. Run dbt pipeline first.")

with tab3:
    if st.button("Load Raw JSON Responses", key="load_raw_json"):
        df = run_query(f"""
            SELECT PATIENT_ID,
                   ICD10_CODES_JSON,
                   BATCH_GROUP_SUMMARY
            FROM {DATABASE}.{DBT_SCHEMA}.LLM_SP_RESULTS 
            ORDER BY PATIENT_ID
            LIMIT 5
        """)
        if not df.empty:
            for _, row in df.iterrows():
                with st.expander(f"Patient {row['PATIENT_ID']}"):
                    st.markdown("**Batch Group Summary:**")
                    st.write(row['BATCH_GROUP_SUMMARY'])
                    st.markdown("**ICD-10 Codes JSON:**")
                    try:
                        codes = json.loads(row['ICD10_CODES_JSON']) if row['ICD10_CODES_JSON'] else []
                        st.json(codes)
                    except:
                        st.code(str(row['ICD10_CODES_JSON']))
        else:
            st.warning("No results found. Run dbt pipeline first.")

st.markdown("---")

# Step 4: Evaluation Results
st.header("Step 4: Evaluation Results")
tab1, tab2, tab3 = st.tabs(["Summary Metrics", "Per-Record Details", "Code Comparison"])

with tab1:
    if st.button("Load Summary Metrics", key="load_summary"):
        df = run_query(f"""
            SELECT EXPERIMENT_NAME, TOTAL_RECORDS, 
                   TOTAL_EXTRACTED_CODES, TOTAL_EXPECTED_CODES,
                   TOTAL_TRUE_POSITIVES, TOTAL_FALSE_POSITIVES, TOTAL_FALSE_NEGATIVES,
                   ROUND(MICRO_PRECISION, 4) AS MICRO_PRECISION,
                   ROUND(MICRO_RECALL, 4) AS MICRO_RECALL,
                   ROUND(MICRO_F1, 4) AS MICRO_F1,
                   EVALUATED_AT
            FROM {DATABASE}.{DBT_SCHEMA}.EVAL_EXPERIMENT_SUMMARY 
            ORDER BY EVALUATED_AT DESC LIMIT 10
        """)
        if not df.empty:
            latest = df.iloc[0]
            
            # Key metrics in large display
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Micro Precision", f"{float(latest.get('MICRO_PRECISION', 0)):.1%}")
            col2.metric("Micro Recall", f"{float(latest.get('MICRO_RECALL', 0)):.1%}")
            col3.metric("Micro F1", f"{float(latest.get('MICRO_F1', 0)):.1%}")
            col4.metric("Patients", int(latest.get('TOTAL_RECORDS', 0)))
            
            # Confusion matrix summary
            st.subheader("Confusion Matrix Summary")
            col1, col2, col3 = st.columns(3)
            col1.metric("True Positives", int(latest.get('TOTAL_TRUE_POSITIVES', 0)))
            col2.metric("False Positives", int(latest.get('TOTAL_FALSE_POSITIVES', 0)))
            col3.metric("False Negatives", int(latest.get('TOTAL_FALSE_NEGATIVES', 0)))
            
            st.dataframe(df, use_container_width=True)
        else:
            st.warning("No results. Run dbt pipeline first.")

with tab2:
    if st.button("Load Per-Record Metrics", key="load_records"):
        df = run_query(f"""
            SELECT PATIENT_ID, 
                   NUM_EXTRACTED, NUM_EXPECTED, 
                   TRUE_POSITIVES, FALSE_POSITIVES, FALSE_NEGATIVES,
                   ROUND(PRECISION_SCORE, 2) AS PRECISION,
                   ROUND(RECALL_SCORE, 2) AS RECALL,
                   ROUND(F1_SCORE, 2) AS F1,
                   CONDITION_NOTES
            FROM {DATABASE}.{DBT_SCHEMA}.EVAL_METRICS_PER_RECORD 
            ORDER BY F1_SCORE DESC
        """)
        if not df.empty:
            # Highlight best and worst performers
            best = df.iloc[0]
            worst = df.iloc[-1]
            
            col1, col2 = st.columns(2)
            with col1:
                st.success(f"**Best:** Patient {best['PATIENT_ID']} - F1: {best['F1']:.0%}")
            with col2:
                st.warning(f"**Needs Review:** Patient {worst['PATIENT_ID']} - F1: {worst['F1']:.0%}")
            
            st.dataframe(df, use_container_width=True, height=400)
        else:
            st.warning("No results. Run dbt pipeline first.")

with tab3:
    if st.button("Load Code Comparison", key="load_comparison"):
        df = run_query(f"""
            SELECT PATIENT_ID,
                   EXTRACTED_CODES_ARRAY,
                   EXPECTED_CODES_ARRAY,
                   TRUE_POSITIVES,
                   FALSE_POSITIVES,
                   FALSE_NEGATIVES
            FROM {DATABASE}.{DBT_SCHEMA}.EVAL_CODE_MATCHES 
            ORDER BY PATIENT_ID
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True, height=400)
        else:
            st.warning("No results. Run dbt pipeline first.")

st.markdown("---")

# Quick Actions
st.header("Quick Actions")
col1, col2, col3 = st.columns(3)

with col1:
    if st.button("View All Tables"):
        df = run_query(f"""
            SELECT TABLE_NAME, ROW_COUNT 
            FROM {DATABASE}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{DBT_SCHEMA}' 
            ORDER BY TABLE_NAME
        """)
        if not df.empty:
            st.dataframe(df, use_container_width=True)

with col2:
    if st.button("Reset Evaluation"):
        tables = ['EVAL_CODE_MATCHES', 'EVAL_METRICS_PER_RECORD', 'EVAL_EXPERIMENT_SUMMARY', 
                  'LLM_SP_RESULTS', 'LLM_EXTRACTED_CODES', 'LLM_CALL_LOG']
        for t in tables:
            session.sql(f"DROP TABLE IF EXISTS {DATABASE}.{DBT_SCHEMA}.{t}").collect()
        st.success("Evaluation tables reset!")

with col3:
    if st.button("View dbt Project"):
        df = run_query(f"SHOW DBT PROJECTS IN SCHEMA {DATABASE}.{SCHEMA}")
        st.dataframe(df, use_container_width=True)

# Session History
if st.session_state.experiment_history:
    st.markdown("---")
    st.subheader("Session History")
    st.dataframe(pd.DataFrame(st.session_state.experiment_history))
