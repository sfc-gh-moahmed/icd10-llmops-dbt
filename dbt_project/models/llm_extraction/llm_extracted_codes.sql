-- Flattened ICD-10 Codes - Extracts individual codes from LLM JSON response
-- Matches production SP output structure for detailed code analysis
{{ config(
    materialized='table',
    schema='ICD10_DEMO'
) }}

WITH source_results AS (
    SELECT
        experiment_id,
        experiment_name,
        patient_id,
        batch_group_range,
        min_date,
        max_date,
        icd10_codes_json,
        batch_group_summary,
        model_used,
        extracted_at
    FROM {{ ref('llm_sp_results') }}
    WHERE icd10_codes_json IS NOT NULL
),

flattened_codes AS (
    SELECT
        r.experiment_id,
        r.experiment_name,
        r.patient_id,
        r.batch_group_range,
        r.min_date,
        r.max_date,
        -- Extract code details from JSON
        f.value:code::VARCHAR AS icd10_code,
        f.value:justification::VARCHAR AS justification,
        f.value:evidence AS evidence_json,
        ARRAY_SIZE(COALESCE(f.value:evidence, ARRAY_CONSTRUCT())) AS evidence_count,
        r.batch_group_summary,
        r.model_used,
        r.extracted_at,
        f.index AS code_index
    FROM source_results r,
    LATERAL FLATTEN(input => r.icd10_codes_json) f
)

SELECT
    experiment_id,
    experiment_name,
    patient_id,
    batch_group_range,
    min_date,
    max_date,
    icd10_code,
    -- Normalize code format (uppercase, no spaces)
    UPPER(TRIM(REGEXP_REPLACE(icd10_code, '[^A-Za-z0-9.]', ''))) AS icd10_code_normalized,
    justification,
    evidence_json,
    evidence_count,
    batch_group_summary,
    model_used,
    extracted_at,
    code_index
FROM flattened_codes
WHERE icd10_code IS NOT NULL
  AND LENGTH(TRIM(icd10_code)) > 0
