{% snapshot snap_property %}

{{
    config(
      target_schema='silver',
      unique_key='property_nk',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('s_dim_property') }}

{% endsnapshot %}
