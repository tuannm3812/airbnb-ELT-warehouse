{% snapshot lga_snapshot %}

{{
    config(
      target_schema='silver',
      unique_key='lga_code',
      strategy='check',
      check_cols=['lga_name']
    )
}}

SELECT * FROM {{ ref('s_dim_lga') }}

{% endsnapshot %}