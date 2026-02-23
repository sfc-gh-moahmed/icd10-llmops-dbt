-- Evaluation: Experiment-level summary metrics
{{ config(
    materialized='table',
    schema='ICD10_DEMO'
) }}

SELECT
    experiment_id,
    experiment_name,
    
    -- Record counts
    COUNT(*) AS total_records,
    SUM(num_extracted) AS total_extracted_codes,
    SUM(num_expected) AS total_expected_codes,
    
    -- Aggregate confusion matrix
    SUM(true_positives) AS total_true_positives,
    SUM(false_positives) AS total_false_positives,
    SUM(false_negatives) AS total_false_negatives,
    
    -- Micro-averaged Precision
    CASE 
        WHEN SUM(true_positives + false_positives) > 0 
        THEN ROUND(SUM(true_positives)::FLOAT / SUM(true_positives + false_positives), 4)
        ELSE 0 
    END AS micro_precision,
    
    -- Micro-averaged Recall
    CASE 
        WHEN SUM(true_positives + false_negatives) > 0 
        THEN ROUND(SUM(true_positives)::FLOAT / SUM(true_positives + false_negatives), 4)
        ELSE 0 
    END AS micro_recall,
    
    -- Micro-averaged F1
    CASE 
        WHEN SUM(true_positives + false_positives) > 0 AND SUM(true_positives + false_negatives) > 0
        THEN ROUND(
            2 * (SUM(true_positives)::FLOAT / SUM(true_positives + false_positives)) * 
                (SUM(true_positives)::FLOAT / SUM(true_positives + false_negatives)) /
            ((SUM(true_positives)::FLOAT / SUM(true_positives + false_positives)) + 
             (SUM(true_positives)::FLOAT / SUM(true_positives + false_negatives))), 4)
        ELSE 0 
    END AS micro_f1,
    
    -- Macro-averaged metrics (average of per-record metrics)
    ROUND(AVG(precision_score), 4) AS macro_precision,
    ROUND(AVG(recall_score), 4) AS macro_recall,
    ROUND(AVG(f1_score), 4) AS macro_f1,
    
    -- Distribution stats
    ROUND(MIN(f1_score), 4) AS min_f1,
    ROUND(MAX(f1_score), 4) AS max_f1,
    ROUND(STDDEV(f1_score), 4) AS stddev_f1,
    
    CURRENT_TIMESTAMP() AS evaluated_at

FROM {{ ref('eval_metrics_per_record') }}
GROUP BY experiment_id, experiment_name
