{% snapshot orders_snapshot %}

{{
    config(
      target_database='dbt_workshop',
      target_schema='snapshots',
      unique_key='id',

      strategy='timestamp',
      updated_at='_etl_load_at',
    )
}}

select * from {{ source('jaffle_shop', 'orders') }}

{% endsnapshot %}

-- Runtime Error
-- Cross-db references allowed only in RA3.* node. (analytics vs dbt_workshop)
-- references:
-- https://github.com/dbt-labs/dbt-core/issues/3179