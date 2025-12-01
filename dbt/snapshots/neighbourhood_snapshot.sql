{% snapshot neighbourhood_snapshot %}

{{
    config(
      target_schema='silver',
      unique_key='neigh_nk',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('s_dim_neighbourhood') }}

{% endsnapshot %}