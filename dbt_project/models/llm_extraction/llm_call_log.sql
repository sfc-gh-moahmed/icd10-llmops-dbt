-- LLM Call Log - tracks each experiment run
{{ config(
    materialized='table',
    schema='ICD10_DEMO'
) }}

SELECT
    '{{ var("experiment_id") }}' AS experiment_id,
    '{{ var("experiment_name") }}' AS experiment_name,
    '{{ var("model_name", "claude-3-5-sonnet") }}' AS model_name,
    CURRENT_TIMESTAMP() AS run_timestamp,
    (SELECT COUNT(*) FROM {{ ref('stg_batched_events') }}) AS num_patients_processed
