-- Evaluation: Per-record metrics (Precision, Recall, F1)
{{ config(
    materialized='table',
    schema='ICD10_DEMO'
) }}

SELECT
    experiment_id,
    experiment_name,
    patient_id,
    num_extracted,
    num_expected,
    true_positives,
    false_positives,
    false_negatives,
    
    -- Precision: TP / (TP + FP)
    CASE 
        WHEN (true_positives + false_positives) > 0 
        THEN ROUND(true_positives::FLOAT / (true_positives + false_positives), 4)
        ELSE 0 
    END AS precision_score,
    
    -- Recall: TP / (TP + FN)
    CASE 
        WHEN (true_positives + false_negatives) > 0 
        THEN ROUND(true_positives::FLOAT / (true_positives + false_negatives), 4)
        ELSE 0 
    END AS recall_score,
    
    -- F1 Score: 2 * (P * R) / (P + R)
    CASE 
        WHEN (true_positives + false_positives) > 0 AND (true_positives + false_negatives) > 0
        THEN ROUND(
            2 * (true_positives::FLOAT / (true_positives + false_positives)) * 
                (true_positives::FLOAT / (true_positives + false_negatives)) /
            ((true_positives::FLOAT / (true_positives + false_positives)) + 
             (true_positives::FLOAT / (true_positives + false_negatives))), 4)
        ELSE 0 
    END AS f1_score,

    extracted_codes_array,
    expected_codes_array,
    condition_notes

FROM {{ ref('eval_code_matches') }}
