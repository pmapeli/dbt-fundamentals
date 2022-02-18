
with customers as ( 
    select * from {{ ref('stg_customers')}}
),

orders as ( 
    select * from {{ ref('stg_orders')}}
),

payments as ( 
    select * from {{ ref('stg_payments')}}
),

fct_orders as (

    select  orders.order_id, 
            customers.customer_id,
            payments.amount
        from customers
        inner join from orders using (customer_id)
        inner join from payments using (order_id)
        

)

select * from fct_orders