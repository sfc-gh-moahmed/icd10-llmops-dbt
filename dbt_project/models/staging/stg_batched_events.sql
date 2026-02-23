-- Staging model for batched patient events
{{ config(materialized='view') }}

SELECT
    AGILON_MEMBER_ID AS patient_id,
    BATCH_NUMBER AS batch_number,
    BATCHED_EVENTS AS clinical_text,
    MIN_EVENT_DATE AS min_event_date,
    MAX_EVENT_DATE AS max_event_date,
    LENGTH(BATCHED_EVENTS) AS text_length,
    LENGTH(BATCHED_EVENTS) / 4 AS estimated_tokens
FROM {{ source('icd10_demo', 'BATCHED_EVENTS') }}
