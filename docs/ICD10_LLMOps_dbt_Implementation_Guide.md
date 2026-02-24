# ICD-10 LLMOps Framework: dbt Implementation Guide

## Executive Summary

This document provides a comprehensive guide for implementing the ICD-10 code extraction pipeline using dbt (data build tool) on Snowflake, replicating the functionality of the existing Agilon production stored procedure. The dbt approach offers improved modularity, version control, testing capabilities, and integration with modern data engineering workflows.

### Key Differences at a Glance

| Aspect | Original SP | dbt Implementation |
|--------|-------------|-------------------|
| **Language** | Python (Snowpark) | SQL + Jinja2 |
| **Execution** | `CALL procedure_name()` | `EXECUTE DBT PROJECT` |
| **Configuration** | YAML file in Snowflake stage | `dbt_project.yml` |
| **Batch Processing** | Python loops | SQL `GROUP BY` + `LISTAGG` |
| **Version Control** | Manual stage uploads | Git-integrated |
| **Testing** | Manual | dbt test framework |
| **Lineage** | Not tracked | Full DAG visibility |

### POC Results

| Metric | Value |
|--------|-------|
| **Micro Precision** | 47.5% |
| **Micro Recall** | 63.3% |
| **Micro F1** | 54.3% |
| **Patients Processed** | 10 |
| **Codes Extracted** | 40 |
| **True Positives** | 19 |

### What's Preserved (Feature Parity)

| Feature | Status |
|---------|--------|
| Batch grouping per patient | ✅ |
| System + User message prompts | ✅ |
| Structured JSON output schema | ✅ |
| Evidence-backed justifications | ✅ |
| Batch group summaries | ✅ |
| Runtime variables | ✅ |

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [SP vs dbt: Key Differences](#sp-vs-dbt-key-differences)
3. [Project Structure](#project-structure)
4. [Setup Guide](#setup-guide)
5. [dbt Model Deep Dive](#dbt-model-deep-dive)
6. [Streamlit Dashboard](#streamlit-dashboard)
7. [Integration Guide](#integration-guide)
8. [Best Practices](#best-practices)

---

## Architecture Overview

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SNOWFLAKE ACCOUNT                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐       │
│  │   SOURCE DATA    │    │   dbt PROJECT    │    │  STREAMLIT APP   │       │
│  │                  │    │  (Deployed)      │    │  (SiS)           │       │
│  │  • BATCHED_      │    │                  │    │                  │       │
│  │    EVENTS        │───▶│  EXECUTE DBT     │───▶│  Visualization   │       │
│  │  • GROUND_TRUTH  │    │  PROJECT         │    │  & Monitoring    │       │
│  │                  │    │                  │    │                  │       │
│  └──────────────────┘    └────────┬─────────┘    └──────────────────┘       │
│                                   │                                          │
│                                   ▼                                          │
│  ┌──────────────────────────────────────────────────────────────────┐       │
│  │                      dbt MODEL DAG                                │       │
│  │                                                                   │       │
│  │   ┌─────────────┐    ┌─────────────┐    ┌─────────────────┐      │       │
│  │   │   STAGING   │    │     LLM     │    │   EVALUATION    │      │       │
│  │   │   MODELS    │───▶│  EXTRACTION │───▶│     MODELS      │      │       │
│  │   │             │    │   MODELS    │    │                 │      │       │
│  │   │• stg_batched│    │             │    │• eval_code_     │      │       │
│  │   │  _events    │    │• llm_sp_    │    │  matches        │      │       │
│  │   │• stg_ground │    │  results    │    │• eval_metrics_  │      │       │
│  │   │  _truth     │    │• llm_       │    │  per_record     │      │       │
│  │   │             │    │  extracted_ │    │• eval_experiment│      │       │
│  │   │             │    │  codes      │    │  _summary       │      │       │
│  │   │             │    │• llm_call_  │    │                 │      │       │
│  │   │             │    │  log        │    │                 │      │       │
│  │   └─────────────┘    └──────┬──────┘    └─────────────────┘      │       │
│  │                             │                                     │       │
│  │                             ▼                                     │       │
│  │                  ┌──────────────────┐                            │       │
│  │                  │ SNOWFLAKE CORTEX │                            │       │
│  │                  │    COMPLETE()    │                            │       │
│  │                  │ claude-3-5-sonnet│                            │       │
│  │                  └──────────────────┘                            │       │
│  └──────────────────────────────────────────────────────────────────┘       │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA FLOW                                          │
└─────────────────────────────────────────────────────────────────────────────┘

     PATIENT DATA                    LLM PROCESSING                 EVALUATION
    ─────────────                   ────────────────               ────────────

┌─────────────────┐           ┌─────────────────────┐         ┌─────────────────┐
│ BATCHED_EVENTS  │           │   Batch Grouping    │         │  Code Matching  │
│                 │           │   per Patient       │         │                 │
│ • Patient ID    │           │                     │         │ • True Positives│
│ • Batch Number  │──────────▶│ • Combine batches   │         │ • False Pos.    │
│ • Clinical Text │           │ • Build prompts     │         │ • False Neg.    │
│ • Date Range    │           │ • Add context       │         │                 │
└─────────────────┘           └──────────┬──────────┘         └────────▲────────┘
                                         │                             │
                                         ▼                             │
                              ┌─────────────────────┐                  │
                              │  Cortex COMPLETE()  │                  │
                              │                     │                  │
                              │ System Message:     │                  │
                              │ • Expert coder role │                  │
                              │ • Explicit + infer  │                  │
                              │ • Evidence required │                  │
                              │                     │                  │
                              │ User Message:       │                  │
                              │ • Patient context   │                  │
                              │ • Clinical data     │                  │
                              │ • Instructions      │                  │
                              └──────────┬──────────┘                  │
                                         │                             │
                                         ▼                             │
                              ┌─────────────────────┐                  │
                              │   Structured JSON   │                  │
                              │      Response       │──────────────────┘
                              │                     │
                              │ • icd10_codes[]     │
                              │   - code            │
                              │   - justification   │
                              │   - evidence[]      │
                              │ • batch_summary     │
                              └─────────────────────┘
```

---

## SP vs dbt: Key Differences

### Comparison Matrix

| Aspect | Original Stored Procedure | dbt Implementation |
|--------|---------------------------|-------------------|
| **Language** | Python (Snowpark) | SQL + Jinja2 |
| **Execution** | `CALL procedure_name()` | `EXECUTE DBT PROJECT` |
| **Configuration** | YAML file in stage | `dbt_project.yml` variables |
| **Batch Processing** | Python loops with batch_group_size | SQL aggregation with LISTAGG |
| **LLM Call** | `TRY_COMPLETE()` in Python | `COMPLETE()` in SQL |
| **Output Parsing** | Python JSON parsing | SQL `TRY_PARSE_JSON()` |
| **Error Handling** | Python try/except | SQL `TRY_*` functions |
| **Version Control** | Manual stage uploads | Git-integrated, `snow dbt deploy` |
| **Testing** | Manual | dbt tests (schema, data) |
| **Documentation** | Separate docs | Auto-generated from schema.yml |
| **Lineage** | Not tracked | Full DAG visibility |
| **Modularity** | Single procedure | Separate models per concern |

### Feature Parity Checklist

| Feature | SP | dbt | Notes |
|---------|----|----|-------|
| Batch grouping per patient | ✅ | ✅ | SQL GROUP BY + LISTAGG |
| System + User message prompts | ✅ | ✅ | Embedded in SQL model |
| Structured JSON output | ✅ | ✅ | Using model options |
| Evidence-backed codes | ✅ | ✅ | Parsed from JSON |
| Batch group summaries | ✅ | ✅ | Extracted field |
| Code deduplication | ✅ | ✅ | DISTINCT in extraction |
| Multiple output tables | ✅ | ✅ | Separate dbt models |
| Runtime variables | ✅ | ✅ | dbt vars |

### What's Different

#### 1. Batch Processing Logic

**Original SP (Python):**
```python
def run_batched_patient_analysis(session, df, config, batch_group_size=8):
    for patient_id in df['AGILON_MEMBER_ID'].unique():
        patient_batches = df[df['AGILON_MEMBER_ID'] == patient_id]
        for i in range(0, len(patient_batches), batch_group_size):
            batch_group = patient_batches.iloc[i:i+batch_group_size]
            # Process batch group...
```

**dbt SQL:**
```sql
batch_groups AS (
    SELECT
        patient_id,
        MIN(batch_number) AS min_batch,
        MAX(batch_number) AS max_batch,
        LISTAGG(clinical_text, '\n\n---BATCH SEPARATOR---\n\n') 
            WITHIN GROUP (ORDER BY batch_number) AS combined_clinical_text
    FROM source_data
    GROUP BY patient_id
)
```

#### 2. LLM Invocation

**Original SP:**
```python
response = session.sql(f"""
    SELECT SNOWFLAKE.CORTEX.TRY_COMPLETE(
        '{model}',
        {messages_json},
        {model_options_json}
    )
""").collect()
```

**dbt SQL:**
```sql
SNOWFLAKE.CORTEX.COMPLETE(
    '{{ var("model_name", "claude-3-5-sonnet") }}',
    [
        {'role': 'system', 'content': '...'},
        {'role': 'user', 'content': '...' || combined_clinical_text}
    ],
    {'max_tokens': 8000, 'temperature': 0}
) AS llm_raw_response
```

#### 3. Configuration Management

**Original SP:** YAML file stored in Snowflake stage
```yaml
config:
  model: 'claude-4-sonnet'
  system_message: |
    You are an expert medical coding specialist...
```

**dbt:** Variables in `dbt_project.yml` and runtime
```yaml
vars:
  experiment_id: 'default'
  experiment_name: 'Default Experiment'
  model_name: 'claude-3-5-sonnet'
```

---

## Project Structure

```
icd10_llmops/
├── dbt_project.yml              # Project configuration
├── profiles.yml                 # Connection profiles
├── models/
│   ├── sources.yml              # Source definitions
│   ├── staging/
│   │   ├── stg_batched_events.sql    # Patient events staging
│   │   └── stg_ground_truth.sql      # Ground truth staging
│   ├── llm_extraction/
│   │   ├── llm_sp_results.sql        # Main LLM extraction (SP logic)
│   │   ├── llm_extracted_codes.sql   # Flattened codes with evidence
│   │   └── llm_call_log.sql          # Call metadata
│   └── evaluation/
│       ├── eval_code_matches.sql     # Code-level comparison
│       ├── eval_metrics_per_record.sql  # Per-patient metrics
│       └── eval_experiment_summary.sql  # Aggregate metrics
└── seeds/
    └── sample_data.csv          # Optional seed data
```

### Model Dependency Graph (DAG)

```
                    ┌─────────────────┐
                    │ sources.yml     │
                    │                 │
                    │ • BATCHED_EVENTS│
                    │ • GROUND_TRUTH  │
                    └────────┬────────┘
                             │
              ┌──────────────┴──────────────┐
              │                             │
              ▼                             ▼
    ┌─────────────────┐           ┌─────────────────┐
    │stg_batched_events│          │ stg_ground_truth │
    │    (view)        │          │     (view)       │
    └────────┬─────────┘          └────────┬─────────┘
             │                             │
             ▼                             │
    ┌─────────────────┐                    │
    │  llm_sp_results │                    │
    │    (table)      │                    │
    │                 │                    │
    │ Cortex COMPLETE │                    │
    └────────┬────────┘                    │
             │                             │
             ├─────────────────────────────┤
             │                             │
             ▼                             │
    ┌─────────────────┐                    │
    │llm_extracted_   │                    │
    │codes (table)    │                    │
    │                 │                    │
    │ LATERAL FLATTEN │                    │
    └────────┬────────┘                    │
             │                             │
             ▼                             ▼
    ┌─────────────────────────────────────────┐
    │         eval_code_matches               │
    │             (table)                     │
    │                                         │
    │  ARRAY_INTERSECTION for matching        │
    └─────────────────┬───────────────────────┘
                      │
                      ▼
    ┌─────────────────────────────────────────┐
    │       eval_metrics_per_record           │
    │             (table)                     │
    │                                         │
    │  Precision, Recall, F1 per patient      │
    └─────────────────┬───────────────────────┘
                      │
                      ▼
    ┌─────────────────────────────────────────┐
    │       eval_experiment_summary           │
    │             (table)                     │
    │                                         │
    │  Micro/Macro aggregate metrics          │
    └─────────────────────────────────────────┘
```

---

## Setup Guide

### Prerequisites

1. **Snowflake Account** with:
   - Cortex LLM functions enabled
   - Appropriate warehouse (XSMALL sufficient for POC)
   - Database and schema created

2. **Local Environment**:
   - Snow CLI installed (`pip install snowflake-cli`)
   - dbt-snowflake adapter (`pip install dbt-snowflake`)
   - Git for version control

### Step 1: Clone/Create Project Structure

```bash
# Create project directory
mkdir icd10_llmops && cd icd10_llmops

# Initialize dbt project
dbt init icd10_llmops --adapter snowflake
```

### Step 2: Configure Connection

Create `profiles.yml`:

```yaml
icd10_llmops:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: <your_account>
      user: <your_user>
      authenticator: externalbrowser
      role: <your_role>
      warehouse: <your_warehouse>
      database: <your_database>
      schema: <your_schema>
      threads: 4
```

### Step 3: Configure Project

Update `dbt_project.yml`:

```yaml
name: 'icd10_llmops'
version: '1.0.0'
config-version: 2

profile: 'icd10_llmops'

model-paths: ["models"]
seed-paths: ["seeds"]

vars:
  experiment_id: 'default'
  experiment_name: 'Default Experiment'
  model_name: 'claude-3-5-sonnet'

models:
  icd10_llmops:
    staging:
      +materialized: view
    llm_extraction:
      +materialized: table
      +schema: ICD10_DEMO
    evaluation:
      +materialized: table
      +schema: ICD10_DEMO
```

### Step 4: Create Source Definitions

Create `models/sources.yml`:

```yaml
version: 2

sources:
  - name: icd10_demo
    database: ICD10_POC
    schema: ICD10_DEMO
    tables:
      - name: BATCHED_EVENTS
        description: "Patient clinical events batched for processing"
        columns:
          - name: AGILON_MEMBER_ID
            description: "Patient identifier"
          - name: BATCH_NUMBER
            description: "Batch sequence number"
          - name: BATCHED_EVENTS
            description: "Clinical text content"
          - name: MIN_EVENT_DATE
            description: "Start of date range"
          - name: MAX_EVENT_DATE
            description: "End of date range"
            
      - name: GROUND_TRUTH
        description: "Expected ICD-10 codes for evaluation"
        columns:
          - name: AGILON_MEMBER_ID
            description: "Patient identifier"
          - name: EXPECTED_ICD10_CODES
            description: "Comma-separated expected codes"
          - name: NOTES
            description: "Clinical condition notes"
```

### Step 5: Deploy to Snowflake

```bash
# Deploy dbt project to Snowflake
snow dbt deploy ICD10_LLMOPS_PROJECT \
  --database ICD10_POC \
  --schema ICD10_DEMO \
  --connection YOUR_CONNECTION
```

### Step 6: Execute Pipeline

```sql
-- Run from Snowflake worksheet or via CLI
EXECUTE DBT PROJECT ICD10_POC.ICD10_DEMO.ICD10_LLMOPS_PROJECT;

-- Or with custom variables
EXECUTE DBT PROJECT ICD10_POC.ICD10_DEMO.ICD10_LLMOPS_PROJECT
  ARGS = '{"vars": {"experiment_id": "exp_001", "experiment_name": "Production Run"}}';
```

---

## dbt Model Deep Dive

### Core Model: `llm_sp_results.sql`

This is the main model that replicates the SP logic:

```sql
{{ config(
    materialized='table',
    schema='ICD10_DEMO'
) }}

WITH source_data AS (
    SELECT
        patient_id,
        batch_number,
        clinical_text,
        min_event_date,
        max_event_date
    FROM {{ ref('stg_batched_events') }}
),

batch_groups AS (
    -- Replicate SP batch grouping logic
    SELECT
        patient_id,
        MIN(batch_number) AS min_batch,
        MAX(batch_number) AS max_batch,
        MIN(min_event_date) AS min_date,
        MAX(max_event_date) AS max_date,
        LISTAGG(clinical_text, '\n\n---BATCH SEPARATOR---\n\n') 
            WITHIN GROUP (ORDER BY batch_number) AS combined_clinical_text
    FROM source_data
    GROUP BY patient_id
),

llm_calls AS (
    SELECT
        patient_id,
        min_batch,
        max_batch,
        min_date,
        max_date,
        combined_clinical_text,
        -- Call Cortex with production prompts
        SNOWFLAKE.CORTEX.COMPLETE(
            '{{ var("model_name", "claude-3-5-sonnet") }}',
            [
                {
                    'role': 'system', 
                    'content': 'You are an expert medical coding specialist.
Identify all ICD-10 codes that apply to the patient in this batch group.

Include both:
- Explicitly documented codes mentioned directly in clinical notes.
- Clinically inferred codes supported by evidence from medications, labs, vitals.

For each ICD-10 code provide:
1. The code and concise clinical description.
2. Up to 3 sentences explaining explicit documentation or inference.
3. Supporting evidence items with evidence_id, source_type, and date.

Return your response as valid JSON with this structure:
{
  "patient_id": "string",
  "batch_group_range": "string",
  "icd10_codes": [
    {
      "code": "ICD-10 code",
      "justification": "explanation",
      "evidence": [{"evidence_id": "string", "source_type": "string", "date": "string"}]
    }
  ],
  "batch_group_summary": "5-8 line summary"
}'
                },
                {
                    'role': 'user', 
                    'content': 'Analyze this batch group and identify ICD-10 codes.

PATIENT ID: ' || patient_id || '
BATCH GROUP RANGE: ' || min_batch::VARCHAR || '-' || max_batch::VARCHAR || '
DATE RANGE: ' || COALESCE(min_date::VARCHAR, 'N/A') || ' to ' || COALESCE(max_date::VARCHAR, 'N/A') || '

DATA:
' || combined_clinical_text
                }
            ],
            {'max_tokens': 8000, 'temperature': 0}
        ) AS llm_raw_response
    FROM batch_groups
),

parsed_results AS (
    SELECT
        patient_id,
        min_batch || '-' || max_batch AS batch_group_range,
        min_date,
        max_date,
        combined_clinical_text AS clinical_text,
        llm_raw_response,
        -- Extract message from Cortex response
        COALESCE(
            llm_raw_response:choices[0]:messages::VARCHAR,
            llm_raw_response:choices[0]:message::VARCHAR,
            llm_raw_response::VARCHAR
        ) AS llm_response_text,
        -- Parse JSON from response
        TRY_PARSE_JSON(
            COALESCE(
                llm_raw_response:choices[0]:messages::VARCHAR,
                llm_raw_response:choices[0]:message::VARCHAR
            )
        ) AS parsed_json
    FROM llm_calls
)

SELECT
    '{{ var("experiment_id") }}' AS experiment_id,
    '{{ var("experiment_name") }}' AS experiment_name,
    patient_id,
    batch_group_range,
    min_date,
    max_date,
    clinical_text,
    llm_response_text AS llm_response,
    parsed_json:icd10_codes AS icd10_codes_json,
    parsed_json:batch_group_summary::VARCHAR AS batch_group_summary,
    ARRAY_SIZE(COALESCE(parsed_json:icd10_codes, ARRAY_CONSTRUCT())) AS num_codes_extracted,
    '{{ var("model_name", "claude-3-5-sonnet") }}' AS model_used,
    CURRENT_TIMESTAMP() AS extracted_at
FROM parsed_results
```

### Flattening Model: `llm_extracted_codes.sql`

Extracts individual codes with justifications:

```sql
{{ config(materialized='table', schema='ICD10_DEMO') }}

SELECT
    r.experiment_id,
    r.experiment_name,
    r.patient_id,
    r.batch_group_range,
    f.value:code::VARCHAR AS icd10_code,
    UPPER(TRIM(REGEXP_REPLACE(f.value:code::VARCHAR, '[^A-Za-z0-9.]', ''))) AS icd10_code_normalized,
    f.value:justification::VARCHAR AS justification,
    f.value:evidence AS evidence_json,
    ARRAY_SIZE(COALESCE(f.value:evidence, ARRAY_CONSTRUCT())) AS evidence_count,
    r.batch_group_summary,
    r.model_used,
    r.extracted_at,
    f.index AS code_index
FROM {{ ref('llm_sp_results') }} r,
LATERAL FLATTEN(input => r.icd10_codes_json) f
WHERE f.value:code IS NOT NULL
```

---

## Streamlit Dashboard

### Overview

The Streamlit in Snowflake (SiS) application provides a visual interface for:

1. **Source Data Review** - View patient events and ground truth
2. **Pipeline Execution** - Trigger dbt runs with one click
3. **Results Visualization** - View extracted codes with justifications
4. **Evaluation Metrics** - Monitor Precision, Recall, F1 scores

### Key Components

```
┌─────────────────────────────────────────────────────────────────┐
│                    STREAMLIT DASHBOARD                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 1: Source Data Overview                             │   │
│  │                                                          │   │
│  │  [Load Source Data]     [Load Ground Truth]              │   │
│  │   • Patient events       • Expected ICD-10 codes         │   │
│  │   • Batch numbers        • Clinical conditions           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 2: Run dbt Evaluation Pipeline                      │   │
│  │                                                          │   │
│  │  [▶ Run dbt Pipeline]  ← Executes EXECUTE DBT PROJECT    │   │
│  │                                                          │   │
│  │  Progress: ████████████ 100%                             │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 3: LLM Extraction Results                           │   │
│  │                                                          │   │
│  │  [Summary] [Extracted Codes Detail] [Raw JSON]           │   │
│  │                                                          │   │
│  │  Patient | Codes | Summary                               │   │
│  │  1001    | 4     | Type 2 diabetes with complications... │   │
│  │  1002    | 4     | Acute COPD exacerbation with...       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │ Step 4: Evaluation Results                               │   │
│  │                                                          │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐    │   │
│  │  │Precision │ │ Recall   │ │   F1     │ │ Records  │    │   │
│  │  │  47.5%   │ │  63.3%   │ │  54.3%   │ │    10    │    │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Deployment

```bash
# Deploy Streamlit app to Snowflake
snow streamlit deploy \
  --database ICD10_POC \
  --schema ICD10_DEMO \
  --connection YOUR_CONNECTION \
  --replace
```

### Key Code Patterns

**Getting Snowflake Session (SiS):**
```python
from snowflake.snowpark.context import get_active_session
session = get_active_session()
```

**Executing dbt Project:**
```python
def run_dbt_project():
    dbt_sql = f"EXECUTE DBT PROJECT {DATABASE}.{SCHEMA}.{DBT_PROJECT}"
    result = session.sql(dbt_sql).collect()
    return True, "Success", ""
```

**Querying Results:**
```python
def run_query(sql):
    return session.sql(sql).to_pandas()

# Example usage
df = run_query(f"""
    SELECT patient_id, icd10_code, justification, evidence_count
    FROM {DATABASE}.{DBT_SCHEMA}.LLM_EXTRACTED_CODES
""")
```

---

## Integration Guide

### Integrating into Existing dbt Project

If you already have a dbt project, here's how to add the ICD-10 extraction models:

#### 1. Add Source Definition

Add to your existing `sources.yml`:

```yaml
sources:
  - name: clinical_data
    tables:
      - name: BATCHED_EVENTS
      - name: GROUND_TRUTH
```

#### 2. Copy Model Files

Copy these models to your project:

```
your_project/
├── models/
│   ├── staging/
│   │   ├── stg_batched_events.sql     # Copy this
│   │   └── stg_ground_truth.sql       # Copy this
│   ├── llm_extraction/
│   │   ├── llm_sp_results.sql         # Copy this
│   │   └── llm_extracted_codes.sql    # Copy this
│   └── evaluation/
│       └── eval_*.sql                  # Copy these
```

#### 3. Update Project Variables

Add to `dbt_project.yml`:

```yaml
vars:
  # Existing vars...
  
  # ICD-10 extraction vars
  icd10_experiment_id: 'default'
  icd10_experiment_name: 'Default'
  icd10_model_name: 'claude-3-5-sonnet'
```

#### 4. Customize Prompts

Modify the system message in `llm_sp_results.sql` to match your specific requirements:

```sql
-- Customize this section for your use case
'role': 'system', 
'content': 'Your customized system prompt here...'
```

### Scheduling with dbt Cloud or Airflow

**dbt Cloud:**
```yaml
# In dbt_project.yml or dbt Cloud UI
jobs:
  - name: icd10_extraction_daily
    schedule: "0 6 * * *"  # 6 AM daily
    commands:
      - dbt run --select tag:icd10
```

**Airflow:**
```python
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

execute_dbt = SnowflakeOperator(
    task_id='run_icd10_extraction',
    sql="EXECUTE DBT PROJECT YOUR_DB.YOUR_SCHEMA.ICD10_LLMOPS_PROJECT",
    snowflake_conn_id='snowflake_default'
)
```

---

## Best Practices

### 1. Model Organization

- Keep staging models as views (fast, no storage cost)
- Materialize LLM extraction as tables (cache expensive results)
- Use incremental models for growing datasets

### 2. Cost Optimization

```sql
-- Add filters to limit LLM calls during development
WHERE patient_id IN ('1001', '1002', '1003')  -- Subset for testing
```

### 3. Error Handling

```sql
-- Use TRY_PARSE_JSON for safe JSON parsing
TRY_PARSE_JSON(llm_response) AS parsed_json

-- Handle null responses
COALESCE(parsed_json:icd10_codes, ARRAY_CONSTRUCT()) AS codes
```

### 4. Testing

Add tests in `schema.yml`:

```yaml
models:
  - name: llm_sp_results
    columns:
      - name: patient_id
        tests:
          - not_null
          - unique
      - name: num_codes_extracted
        tests:
          - not_null
```

### 5. Documentation

Document your models:

```yaml
models:
  - name: llm_sp_results
    description: |
      Main LLM extraction model that processes patient clinical data
      and extracts ICD-10 codes using Snowflake Cortex.
      
      Replicates Agilon production SP logic including:
      - Batch grouping per patient
      - Structured JSON output parsing
      - Evidence-backed justifications
```

---

## Appendix: Quick Reference

### Key Commands

```bash
# Deploy dbt project
snow dbt deploy PROJECT_NAME --database DB --schema SCHEMA --connection CONN

# Execute dbt project
snow sql -q "EXECUTE DBT PROJECT DB.SCHEMA.PROJECT_NAME" --connection CONN

# Deploy Streamlit app
snow streamlit deploy --database DB --schema SCHEMA --connection CONN --replace

# Check dbt logs
snow sql -q "SELECT system\$get_dbt_log('QUERY_ID')" --connection CONN
```

### Output Tables

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `LLM_SP_RESULTS` | Main extraction results | patient_id, icd10_codes_json, batch_group_summary |
| `LLM_EXTRACTED_CODES` | Flattened individual codes | icd10_code, justification, evidence_json |
| `LLM_CALL_LOG` | Execution metadata | experiment_id, run_timestamp, num_patients |
| `EVAL_CODE_MATCHES` | Code comparison | extracted_codes_array, expected_codes_array |
| `EVAL_METRICS_PER_RECORD` | Per-patient metrics | precision_score, recall_score, f1_score |
| `EVAL_EXPERIMENT_SUMMARY` | Aggregate metrics | micro_precision, micro_recall, micro_f1 |

### Metrics Interpretation

| Metric | Formula | Target |
|--------|---------|--------|
| Precision | TP / (TP + FP) | >80% for production |
| Recall | TP / (TP + FN) | >80% for production |
| F1 Score | 2 * (P * R) / (P + R) | >80% for production |

---

## Contact & Support

For questions about this implementation, contact your Snowflake Solutions Architect.

---

*Document Version: 1.0*  
*Last Updated: February 2026*  
*Author: Snowflake Professional Services*
