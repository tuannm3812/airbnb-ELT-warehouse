{% snapshot host_snapshot %}

{{
    config(
      target_schema='silver',
      unique_key='host_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('s_dim_host') }}

{% endsnapshot %}