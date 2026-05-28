{% snapshot snap_host %}

{{
    config(
      target_schema='silver',
      unique_key='host_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    updated_at
FROM {{ ref('s_dim_host') }}

{% endsnapshot %}
