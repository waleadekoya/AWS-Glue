{{ config(materialized='view') }}

with customers as (

    select * from {{ ref('stg_customers') }}

),

     orders as (

         select * from {{ ref('stg_orders') }}

     ),

    first_pymt as (

            select
                   customer_id,
                   order_date as first_order_date,
                   payment_amount as first_order_amt
            from orders
    ),

    most_recent_pymt as (

            select
                   customer_id,
                   order_date as most_recent_order_date,
                   payment_amount as most_recent_order_amt
            from orders
    ),

     customer_orders as (

         select
             customer_id,

             min(order_date) as first_order_date,
             max(order_date) as most_recent_order_date,
             count(order_id) as number_of_orders

         from orders

         group by 1

     ),

     final as (

         select
             customers.customer_id,
             customers.first_name,
             customers.last_name,
             customer_orders.first_order_date,
             customer_orders.most_recent_order_date,
             first_pymt.first_order_amt,
             most_recent_pymt.most_recent_order_amt,
             coalesce(customer_orders.number_of_orders, 0) as number_of_orders

         from customers

                  left join customer_orders using (customer_id)
                  left join first_pymt using (customer_id, first_order_date)
                  left join most_recent_pymt using (customer_id, most_recent_order_date)

     )

select * from final
