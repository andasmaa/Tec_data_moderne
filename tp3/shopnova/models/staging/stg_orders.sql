select
    order_id,
    cast(customer_id as integer)     as customer_id,
    cast(order_ts as timestamp)      as ordered_at,
    cast(order_ts as date)           as order_date,
    cast(amount as decimal(12, 2))   as amount,
    upper(status)                    as status
from {{ source('silver', 'orders') }}