{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id',
    incremental_strategy = 'delete+insert'
) }}

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.ordered_at,
    o.status,
    o.amount,
    s.is_revenue
from {{ ref('stg_orders') }} o
join {{ ref('order_status') }} s using (status)

{% if is_incremental() %}
  -- fenêtre de 3 jours pour rattraper les événements arrivés en retard
  where o.ordered_at > (select max(ordered_at) from {{ this }}) - interval 3 day
{% endif %}
