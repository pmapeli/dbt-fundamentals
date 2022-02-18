
with customers as ( 
    select * from {{ ref('stg_customers')}}
),

orders as ( 
    select * from {{ ref('stg_orders')}}
),

payments as ( 
    select * from {{ ref('stg_payments')}}
),

customer_order as (

    select  orders.order_id, 
            customers.customer_id
        from customers
        inner join orders using (customer_id)

),

order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount

    from payments
    group by 1
),

final as (

    select  customer_order.order_id,
            customer_order.customer_id,
            coalesce(order_payments.amount, 0) as amount
        from customer_order
        left join order_payments using (order_id)
        
)

select * from final

