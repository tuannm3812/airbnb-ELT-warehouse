{% snapshot snap_neighbourhood %}

{{
    config(
      target_schema='silver',
      unique_key='neigh_nk',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT
    neigh_nk,
    listing_neighbourhood,
    updated_at
FROM {{ ref('s_dim_neighbourhood') }}

{% endsnapshot %}
