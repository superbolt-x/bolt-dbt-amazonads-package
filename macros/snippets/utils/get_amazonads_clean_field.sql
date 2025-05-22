{%- macro get_amazonads_clean_field(table_name, column_name) -%}

    {#- /* Apply to specific table */ -#}
    {%- if "profile" in table_name -%}

        {%- if column_name in ("id","currency_code") -%}
        {{column_name}} as profile_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "campaign" in table_name -%}

        {%- if column_name in ("id","name","state","serving_status") -%}
        {{column_name}} as campaign_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "ad_group" in table_name -%}

        {%- if column_name in ("id","name","state","serving_status") -%}
        {{column_name}} as ad_group_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "keyword" in table_name -%}

        {%- if column_name in ("id","match_type","state","bid") -%}
        {{column_name}} as keyword_{{column_name}}

        {%- elif column_name in ("report_date") -%}
        {{column_name}} as date

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}


    {%- else -%}
    {{column_name}}
        
    {%- endif -%}

{% endmacro -%}
