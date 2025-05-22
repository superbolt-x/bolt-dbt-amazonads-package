{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}


{%- set schema_name, insights_table_name = 'amazonads_raw', 'sb_keyword_report' -%}
{%- set insights_exclude_fields = [
   "currency",
   "applicable_budget_rule_id"
]
-%}

{%- set insights_fields = adapter.get_columns_in_relation(source(schema_name, insights_table_name))
                    |map(attribute="name")
                    |reject("in",insights_exclude_fields)
                    -%}  

WITH insights AS 
    (SELECT 
        {%- for field in insights_fields %}
        {{ get_amazonads_clean_field(insights_table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, insights_table_name) }}
    )

SELECT *,
    keyword_id||'_'||ad_group_id||'_'||campaign_id||'_'||date as unique_key
FROM insights
{% if is_incremental() -%}

where date >= (select max(date)-30 from {{ this }})

{% endif %}
