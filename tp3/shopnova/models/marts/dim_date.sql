select
    cast(d as date)              as date_day,
    year(d)                      as year,
    month(d)                     as month,
    strftime(d, '%Y-%m')         as year_month,
    dayofweek(d) in (0, 6)       as is_weekend
from generate_series(date '2024-01-01', date '2027-12-31', interval 1 day) as t(d)
