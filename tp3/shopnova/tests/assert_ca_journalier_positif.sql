-- Le test échoue s'il renvoie au moins une ligne
select order_date, sum(amount) as ca
from {{ ref('fct_orders') }}
group by order_date
having sum(amount) < 0