select
    f.order_date,
    c.segment,
    count(*)                                  as nb_commandes,
    sum(f.amount) filter (where f.is_revenue) as chiffre_affaires,
    round(avg(f.amount), 2)                   as panier_moyen
from {{ ref('fct_orders') }} f
join {{ ref('dim_customers') }} c using (customer_id)
group by all
