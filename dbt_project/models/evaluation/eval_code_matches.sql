-- Evaluation: Code-level matching between extracted and expected
-- Updated to use structured JSON output from production SP logic
{{ config(
    materialized='table',
    schema='ICD10_DEMO'
) }}

WITH extracted_codes AS (
    -- Get normalized codes from the flattened extraction results
    SELECT
        experiment_id,
        experiment_name,
        patient_id,
        icd10_code_normalized AS extracted_code,
        justification,
        evidence_count
    FROM {{ ref('llm_extracted_codes') }}
),

extracted_by_patient AS (
    -- Aggregate extracted codes per patient
    SELECT
        experiment_id,
        experiment_name,
        patient_id,
        ARRAY_AGG(DISTINCT extracted_code) AS extracted_codes_array,
        COUNT(DISTINCT extracted_code) AS num_extracted
    FROM extracted_codes
    GROUP BY experiment_id, experiment_name, patient_id
),

ground_truth AS (
    SELECT 
        patient_id,
        expected_codes_array,
        expected_codes,
        condition_notes
    FROM {{ ref('stg_ground_truth') }}
),

-- Join with ground truth and create code-level comparisons
code_comparison AS (
    SELECT
        e.experiment_id,
        e.experiment_name,
        e.patient_id,
        e.extracted_codes_array,
        g.expected_codes_array,
        g.expected_codes,
        g.condition_notes,
        e.num_extracted,
        ARRAY_SIZE(COALESCE(g.expected_codes_array, ARRAY_CONSTRUCT())) AS num_expected
    FROM extracted_by_patient e
    LEFT JOIN ground_truth g ON e.patient_id = g.patient_id
)

SELECT
    experiment_id,
    experiment_name,
    patient_id,
    extracted_codes_array,
    expected_codes_array,
    expected_codes,
    condition_notes,
    num_extracted,
    num_expected,
    -- Calculate true positives (codes that match)
    ARRAY_SIZE(ARRAY_INTERSECTION(extracted_codes_array, COALESCE(expected_codes_array, ARRAY_CONSTRUCT()))) AS true_positives,
    -- False positives (extracted but not expected)
    num_extracted - ARRAY_SIZE(ARRAY_INTERSECTION(extracted_codes_array, COALESCE(expected_codes_array, ARRAY_CONSTRUCT()))) AS false_positives,
    -- False negatives (expected but not extracted)
    num_expected - ARRAY_SIZE(ARRAY_INTERSECTION(extracted_codes_array, COALESCE(expected_codes_array, ARRAY_CONSTRUCT()))) AS false_negatives
FROM code_comparison
