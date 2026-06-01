{# =====================================================================
   Cross-dialect helpers — Postgres ↔ Snowflake.

   Postgres is the local dev target; Snowflake is the cloud DW
   target. dbt provides a few adapter-agnostic helpers
   (`dbt.dateadd`, `dbt.date_trunc`) but doesn't ship cross-dialect
   equivalents for FILTER (WHERE) clauses, BOOL_OR, or ordered
   ARRAY_AGG subscripts — the macros below fill that gap so every
   model stays single-source.

   When a future model needs a new dialect-specific construct, add a
   macro here rather than scattering `{% if target.type == ... %}`
   blocks across model files.
   ===================================================================== #}


{# ---------------------------------------------------------------------
   count_if — count rows where condition is true.
   Postgres: COUNT(*) FILTER (WHERE cond)
   Snowflake: COUNT(CASE WHEN cond THEN 1 END)
   --------------------------------------------------------------------- #}
{% macro count_if(condition) -%}
COUNT(CASE WHEN {{ condition }} THEN 1 END)
{%- endmacro %}


{# ---------------------------------------------------------------------
   sum_if — sum values where condition is true; rows that don't match
   contribute 0 (not NULL), so the result is never NULL on an empty
   match (matches Postgres SUM ... FILTER semantics).
   --------------------------------------------------------------------- #}
{% macro sum_if(value, condition) -%}
SUM(CASE WHEN {{ condition }} THEN {{ value }} ELSE 0 END)
{%- endmacro %}


{# ---------------------------------------------------------------------
   avg_if — mean of values where condition is true. Rows where the
   condition is false yield NULL inside the CASE, which AVG skips —
   identical semantics to Postgres `AVG(x) FILTER (WHERE c)`.
   --------------------------------------------------------------------- #}
{% macro avg_if(value, condition) -%}
AVG(CASE WHEN {{ condition }} THEN {{ value }} END)
{%- endmacro %}


{# ---------------------------------------------------------------------
   bool_or_agg — boolean aggregate "any row true".
   Postgres: BOOL_OR(x)
   Snowflake: BOOLOR_AGG(x)
   --------------------------------------------------------------------- #}
{% macro bool_or_agg(column) -%}
{%- if target.type == 'snowflake' -%}
BOOLOR_AGG({{ column }})
{%- else -%}
BOOL_OR({{ column }})
{%- endif -%}
{%- endmacro %}


{# ---------------------------------------------------------------------
   array_first_ordered — pick the first element of an ORDER-BY'd
   array. Postgres uses 1-indexed (array_agg(x ORDER BY y))[1];
   Snowflake uses 0-indexed WITHIN GROUP syntax.
   --------------------------------------------------------------------- #}
{% macro array_first_ordered(value, order_by) -%}
{%- if target.type == 'snowflake' -%}
(ARRAY_AGG({{ value }}) WITHIN GROUP (ORDER BY {{ order_by }}))[0]
{%- else -%}
(ARRAY_AGG({{ value }} ORDER BY {{ order_by }}))[1]
{%- endif -%}
{%- endmacro %}


{# ---------------------------------------------------------------------
   bool_to_int — 1 if expr is true, 0 if false, NULL if NULL. Postgres
   accepts `bool::int` directly; Snowflake needs an explicit CASE.
   --------------------------------------------------------------------- #}
{% macro bool_to_int(expression) -%}
CASE WHEN {{ expression }} THEN 1 WHEN NOT ({{ expression }}) THEN 0 END
{%- endmacro %}


{# ---------------------------------------------------------------------
   to_date — cast to DATE. Snowflake's DATE_TRUNC returns
   TIMESTAMP_NTZ; on Postgres the ::date cast collapses to date.
   This wrapper is the single place to express that.
   --------------------------------------------------------------------- #}
{% macro to_date(expression) -%}
CAST({{ expression }} AS DATE)
{%- endmacro %}


{# ---------------------------------------------------------------------
   dedupe_latest — pick the single latest row per natural-key partition.

   Snowflake raw tables do NOT enforce primary keys; Snowpipe ingests
   every parquet that lands in S3, so re-uploads or overlapping daily
   windows can leave duplicate rows in NEM.RAW.*. Staging picks the
   most-recently-ingested row per natural key and drops the rest.

   Postgres raw tables DO enforce PKs (ON CONFLICT DO NOTHING in the
   loaders), so no dedup is needed there — the macro renders to a
   plain SELECT *.

   Usage:
     {{ dedupe_latest(source('raw', 'region_5min'),
                      ['settlementdate', 'regionid']) }}
   --------------------------------------------------------------------- #}
{% macro dedupe_latest(relation, partition_by) -%}
{%- if target.type == 'snowflake' -%}
SELECT *
FROM {{ relation }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY {{ partition_by | join(', ') }}
    ORDER BY _ingested_at DESC
) = 1
{%- else -%}
SELECT * FROM {{ relation }}
{%- endif -%}
{%- endmacro %}
