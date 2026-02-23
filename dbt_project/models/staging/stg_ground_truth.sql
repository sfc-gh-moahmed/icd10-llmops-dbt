-- Staging model for ground truth ICD-10 codes
{{ config(materialized='view') }}

SELECT
    AGILON_MEMBER_ID AS patient_id,
    EXPECTED_ICD10_CODES AS expected_codes,
    NOTES AS condition_notes,
    -- Parse codes into array for easier comparison
    SPLIT(REPLACE(EXPECTED_ICD10_CODES, ' ', ''), ',') AS expected_codes_array,
    ARRAY_SIZE(SPLIT(REPLACE(EXPECTED_ICD10_CODES, ' ', ''), ',')) AS num_expected_codes
FROM {{ source('icd10_demo', 'GROUND_TRUTH') }}
