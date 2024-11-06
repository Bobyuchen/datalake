{{ config(materialized='view') }}

{% set source_relation = source('iceberg', 'observation') %}
{% set columns = adapter.get_columns_in_relation(source_relation) %}

with source as (
    select 
        {% for column in columns %}
            {{ column.name }} as "{{ column.name | lower }}"{{ "," if not loop.last }}
        {% endfor %}
    from {{ source_relation }}
)

select * from source
