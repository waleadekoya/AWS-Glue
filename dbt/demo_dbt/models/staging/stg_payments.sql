{{ config(materialized='view') }}
select
    order_id,
    sum(amount) as payment_amount

-- from stripe.payments
from {{ source('stripe', 'payments') }}
where status = 'success'
group by order_id
