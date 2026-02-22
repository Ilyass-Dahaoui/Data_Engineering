{% snapshot dim_apps_snapshot %}
{{
  config(
    target='dev',
    strategy='check',
    check_cols=['category', 'title', 'developer_id'],
    unique_key='app_id'
  )
}}

select * from {{ ref('stg_apps') }}

{% endsnapshot %}
