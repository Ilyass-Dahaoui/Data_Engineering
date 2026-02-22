{% snapshot dim_apps_snapshot %}
{
  "target": "dev",
  "strategy": "check",
  "check_cols": ["category", "title", "developer_id"]
}

select * from {{ ref('stg_apps') }}

{% endsnapshot %}
