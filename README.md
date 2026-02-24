# ICD-10 LLMOps dbt Framework - Quick Start Guide

## What's Included

```
customer_package/
├── README.md                 # This file
├── dbt_project/              # Complete dbt project
│   ├── dbt_project.yml
│   ├── profiles.yml.template
│   └── models/
├── streamlit_app/            # Streamlit dashboard
│   ├── streamlit_app.py
│   └── environment.yml
└── docs/                     # Full documentation
    ├── ICD10_LLMOps_dbt_Implementation_Guide.md
    └── ICD10_LLMOps_dbt_Implementation.pptx
```

---

## Quick Setup (5 Steps)

### Step 1: Configure Snowflake Connection

```bash
# Rename and edit the profiles template
cd dbt_project
cp profiles.yml.template profiles.yml
```

Edit `profiles.yml` with your Snowflake credentials:
```yaml
icd10_llmops:
  target: dev
  outputs:
    dev:
      type: snowflake
      account: YOUR_ACCOUNT        # e.g., xy12345.us-east-1
      user: YOUR_USER
      # DO NOT add authenticator: externalbrowser - causes SHOW PARAMETER error in native dbt
      role: YOUR_ROLE
      warehouse: YOUR_WAREHOUSE
      database: YOUR_DATABASE
      schema: YOUR_SCHEMA
      threads: 4
```

### Step 2: Update Source Tables

Edit `models/sources.yml` to point to your data:
```yaml
sources:
  - name: icd10_demo
    database: YOUR_DATABASE      # Change this
    schema: YOUR_SCHEMA          # Change this
    tables:
      - name: BATCHED_EVENTS     # Your patient data table
      - name: GROUND_TRUTH       # Your expected codes table
```

### Step 3: Deploy dbt Project

```bash
# Install Snow CLI if needed
pip install snowflake-cli

# Deploy to Snowflake
snow dbt deploy ICD10_LLMOPS_PROJECT \
  --database YOUR_DATABASE \
  --schema YOUR_SCHEMA \
  --connection YOUR_CONNECTION
```

### Step 4: Run the Pipeline

```sql
-- Execute from Snowflake worksheet
EXECUTE DBT PROJECT YOUR_DATABASE.YOUR_SCHEMA.ICD10_LLMOPS_PROJECT;
```

Or with custom variables:
```sql
EXECUTE DBT PROJECT YOUR_DATABASE.YOUR_SCHEMA.ICD10_LLMOPS_PROJECT
  ARGS = '{"vars": {"experiment_id": "exp_001", "experiment_name": "My Experiment"}}';
```

### Step 5: Deploy Streamlit Dashboard (Optional)

```bash
cd streamlit_app

# Update database/schema in streamlit_app.py lines 14-17
# Then deploy:
snow streamlit deploy \
  --database YOUR_DATABASE \
  --schema YOUR_SCHEMA \
  --connection YOUR_CONNECTION
```

---

## Output Tables

After running, these tables are created:

| Table | Description |
|-------|-------------|
| `LLM_SP_RESULTS` | Main extraction results with JSON codes |
| `LLM_EXTRACTED_CODES` | Flattened codes with justifications |
| `LLM_CALL_LOG` | Execution metadata |
| `EVAL_CODE_MATCHES` | Code comparison vs ground truth |
| `EVAL_METRICS_PER_RECORD` | Per-patient precision/recall/F1 |
| `EVAL_EXPERIMENT_SUMMARY` | Aggregate metrics |

---

## Key Commands Reference

```bash
# Deploy dbt project
snow dbt deploy PROJECT_NAME --database DB --schema SCHEMA --connection CONN

# Execute dbt project
snow sql -q "EXECUTE DBT PROJECT DB.SCHEMA.PROJECT_NAME" --connection CONN

# Check results
snow sql -q "SELECT * FROM DB.SCHEMA.EVAL_EXPERIMENT_SUMMARY" --connection CONN

# Deploy Streamlit
snow streamlit deploy --database DB --schema SCHEMA --connection CONN --replace
```

---

## Customization

### Change LLM Model
Edit `dbt_project.yml`:
```yaml
vars:
  model_name: 'claude-3-5-sonnet'  # or 'llama3.1-70b', etc.
```

### Modify Prompts
Edit `models/llm_extraction/llm_sp_results.sql` - look for the `'role': 'system'` section.

---

## Support

For detailed documentation, see:
- `docs/ICD10_LLMOps_dbt_Implementation_Guide.md`
- `docs/ICD10_LLMOps_dbt_Implementation.pptx`
