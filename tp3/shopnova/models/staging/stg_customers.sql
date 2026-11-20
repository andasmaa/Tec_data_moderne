-- NB : DuckDB n'a pas de fonction initcap(), on met la 1re lettre en majuscule à la main
select
    cast(customer_id as integer)   as customer_id,
    trim(full_name)                as full_name,
    upper(left(trim(city), 1)) || lower(substr(trim(city), 2)) as city,
    upper(segment)                 as segment,
    cast(signup_date as date)      as signup_date
from {{ source('silver', 'customers') }}
