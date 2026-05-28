{% snapshot snap_lga %}

{{
    config(
      target_schema='silver',
      unique_key='lga_code',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT * FROM {{ ref('s_dim_lga') }}

{% endsnapshot %}
