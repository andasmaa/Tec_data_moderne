select
    customer_id,
    full_name,
    city,
    segment,
    signup_date,
    date_diff('day', signup_date, current_date) as anciennete_jours
from {{ ref('stg_customers') }}
