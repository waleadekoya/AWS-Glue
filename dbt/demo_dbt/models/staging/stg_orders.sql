{{ config(materialized='view') }}
select
    o.id as order_id,
    o.user_id as customer_id,
    o.order_date,
    o.status,
    p.payment_amount

-- from jaffle_shop.orders o
from {{ source('jaffle_shop', 'orders') }} o
         left join {{ ref('stg_payments') }} p on o.id = p.order_id