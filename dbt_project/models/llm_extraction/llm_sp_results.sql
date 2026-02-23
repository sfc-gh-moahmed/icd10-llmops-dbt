-- LLM Extraction Results - Replicates Agilon Production SP Logic
-- Uses COMPLETE with structured prompts matching production
{{ config(
    materialized='table',
    schema='ICD10_DEMO'
) }}

WITH source_data AS (
    -- Get batched events from staging
    SELECT
        patient_id,
        batch_number,
        clinical_text,
        min_event_date,
        max_event_date
    FROM {{ ref('stg_batched_events') }}
),

batch_groups AS (
    -- Group batches per patient (matching SP batch_group_size logic)
    SELECT
        patient_id,
        MIN(batch_number) AS min_batch,
        MAX(batch_number) AS max_batch,
        MIN(min_event_date) AS min_date,
        MAX(max_event_date) AS max_date,
        LISTAGG(clinical_text, '\n\n---BATCH SEPARATOR---\n\n') WITHIN GROUP (ORDER BY batch_number) AS combined_clinical_text
    FROM source_data
    GROUP BY patient_id
),

llm_calls AS (
    -- Call Cortex COMPLETE with production prompts
    SELECT
        patient_id,
        min_batch,
        max_batch,
        min_date,
        max_date,
        combined_clinical_text,
        SNOWFLAKE.CORTEX.COMPLETE(
            '{{ var("model_name", "claude-3-5-sonnet") }}',
            [
                {
                    'role': 'system', 
                    'content': 'You are an expert medical coding specialist.
Identify all ICD-10 codes that apply to the patient in this batch group.

Include both:
- Explicitly documented codes mentioned directly in clinical notes, conditions, or diagnoses.
- Clinically inferred codes logically supported by evidence found across clinical notes and documents, medications, labs, vitals, or treatment patterns.

For each ICD-10 code provide:
1. The code and concise clinical description.
2. Up to 3 short sentences explaining whether it is explicitly documented or inferred and why the evidence supports it.
3. Supporting evidence items with evidence_id, source_type, and date.

Return your response as valid JSON with this structure:
{
  "patient_id": "string",
  "batch_group_range": "string (e.g. 1-8)",
  "icd10_codes": [
    {
      "code": "ICD-10 code",
      "justification": "explanation",
      "evidence": [{"evidence_id": "string", "source_type": "string", "date": "string"}]
    }
  ],
  "batch_group_summary": "5-8 line summary of key findings"
}

Keep language factual and structured. Stay concise. Return strictly valid JSON.'
                },
                {
                    'role': 'user', 
                    'content': 'Analyze this batch group of patient data and identify all clinically appropriate ICD-10 codes.

PATIENT ID: ' || patient_id || '
BATCH GROUP RANGE: ' || min_batch::VARCHAR || '-' || max_batch::VARCHAR || '
DATE RANGE: ' || COALESCE(min_date::VARCHAR, 'N/A') || ' to ' || COALESCE(max_date::VARCHAR, 'N/A') || '

DATA:
' || combined_clinical_text || '

Instructions:
- Include every explicit ICD-10 code found in the data.
- Also include inferred codes when strong clinical reasoning supports them.
- Each code must include: code, justification, and supporting evidence.
- Return valid JSON following the schema provided.'
                }
            ],
            {
                'max_tokens': 8000,
                'temperature': 0
            }
        ) AS llm_raw_response
    FROM batch_groups
),

parsed_results AS (
    -- Parse the LLM response and extract structured data
    SELECT
        patient_id,
        min_batch || '-' || max_batch AS batch_group_range,
        min_date,
        max_date,
        combined_clinical_text AS clinical_text,
        llm_raw_response,
        -- Handle COMPLETE response structure (returns object with 'choices' array, 'messages' field)
        COALESCE(
            llm_raw_response:choices[0]:messages::VARCHAR,
            llm_raw_response:choices[0]:message::VARCHAR,
            llm_raw_response::VARCHAR
        ) AS llm_response_text,
        -- Extract structured fields from response - parse the JSON from the message
        TRY_PARSE_JSON(
            COALESCE(
                llm_raw_response:choices[0]:messages::VARCHAR,
                llm_raw_response:choices[0]:message::VARCHAR,
                llm_raw_response::VARCHAR
            )
        ) AS parsed_json
    FROM llm_calls
)

-- Final output matching production SP schema
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
