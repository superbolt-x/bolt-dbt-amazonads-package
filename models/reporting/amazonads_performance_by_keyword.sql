{{ config (
    alias = target.database + '_amazonads_performance_by_keyword'
)}}

{%- set currency_fields = [
    "cost"
]
-%}

{%- set exclude_fields = [
    "unique_key",
    "_fivetran_synced"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_amazonads_keywords_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH 
    {% if var('currency') != 'USD' -%}
    currency AS
    (SELECT DISTINCT date, "{{ var('currency') }}" as raw_rate, 
        LAG(raw_rate) ignore nulls over (order by date) as exchange_rate
    FROM utilities.dates 
    LEFT JOIN utilities.currency USING(date)
    WHERE date <= current_date),
    {%- endif -%}

    {%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}

    insights AS 
    (SELECT 
        {%- for field in stg_fields -%}
        {%- if field in currency_fields or '_sales' in field %}
        "{{ field }}"::float/{{ exchange_rate }} as "{{ field }}"
        {%- else %}
        "{{ field }}"
        {%- endif -%}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ ref('_stg_amazonads_keywords_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    ),

    insights_stg AS 
    (SELECT *,
    {{ get_date_parts('date') }}
    FROM insights),
  
  {%- set selected_fields = [
    "campaign_id",
    "ad_group_id",
    "id",
    "keyword_text",
    "match_type",
    "state",
    "bid"
] -%}

{%- set schema_name, keyword_table_name = 'amazonads_raw', 'keywords' -%}

    keywords_staging AS 
    (SELECT 
        {% for field in selected_fields -%}
        {{ get_amazonads_clean_field(keyword_table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM {{ source(schema_name, keyword_table_name) }})
    ),

  
  {%- set selected_fields = [
    "campaign_id",
    "id",
    "name",
    "state",
    "serving_status",
    "last_updated_date"
] -%}

{%- set schema_name, ad_group_table_name = 'amazonads_raw', 'ad_groups' -%}

    ad_groups_staging AS 
    (SELECT 
        {% for field in selected_fields|reject("eq","last_updated_date") -%}
        {{ get_amazonads_clean_field(ad_group_table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM 
        (SELECT
            {{ selected_fields|join(", ") }},
            MAX(last_updated_date) OVER (PARTITION BY campaign_id, id) as last_updated_at
        FROM {{ source(schema_name, ad_group_table_name) }})
    WHERE last_updated_date = last_updated_at
    ),

{%- set selected_fields = [
    "profile_id",
    "id",
    "name",
    "state",
    "serving_status",
    "last_updated_date"
] -%}
{%- set schema_name, table_name = 'amazonads_raw', 'campaigns' -%}

    campaigns_staging AS 
    (SELECT 
        {% for field in selected_fields|reject("eq","last_updated_date") -%}
        {{ get_amazonads_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM 
        (SELECT
            {{ selected_fields|join(", ") }},
            MAX(last_updated_date) OVER (PARTITION BY id) as last_updated_at
        FROM {{ source(schema_name, table_name) }})
    WHERE last_updated_date = last_updated_at
    ),

{%- set selected_fields = [
    "id",
    "account_name",
    "currency_code"
] -%}
{%- set schema_name, table_name = 'amazonads_raw', 'profile' -%}

    accounts_staging AS 
    (SELECT 
        {% for field in selected_fields -%}
        {{ get_amazonads_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM 
        (SELECT
            id,
            account_name,
            currency_code
        FROM {{ source(schema_name, table_name) }})
    ),

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','unique_key','_fivetran_synced'] -%}
{%- set dimensions = ['campaign_id','ad_group_id','keyword_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('_stg_amazonads_keywords_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}  
 
    {%- for date_granularity in date_granularity_list %}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        {%- for dimension in dimensions %}
        {{ dimension }}::VARCHAR as "{{ dimension }}",
        {%-  endfor %}
        {% for measure in measures -%}
        COALESCE(SUM("{{ measure }}"),0) as "{{ measure }}"
        {%- if not loop.last %},{%- endif %}
        {% endfor %}
    FROM insights_stg
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}
    
    keywords AS
    (SELECT keyword_id, ad_group_id, campaign_id, keyword_text, keyword_state, keyword_match_type, keyword_bid
    FROM keywords_staging),
  
    ad_groups AS
    (SELECT ad_group_id, campaign_id, ad_group_name, ad_group_state
    FROM ad_groups_staging),
    
    campaigns AS
    (SELECT profile_id, campaign_id, campaign_name, campaign_state
    FROM campaigns_staging),
    
    accounts AS
    (SELECT profile_id, account_name
    FROM accounts_staging)

SELECT *,
    date||'_'||date_granularity||'_'||keyword_id||'_'||ad_group_id||'_'||campaign_id as unique_key
FROM 
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
  
LEFT JOIN keywords USING(keyword_id,ad_group_id,campaign_id)
LEFT JOIN ad_groups USING(ad_group_id,campaign_id)
LEFT JOIN campaigns USING(campaign_id)
LEFT JOIN accounts USING(profile_id)
