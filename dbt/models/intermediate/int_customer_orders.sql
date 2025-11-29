{{
    config(
        materialized='table'
    )
}}

with customers as (
    select * from {{ ref('stg_customers') }}
),

sales_orders as (
    select * from {{ ref('stg_sales_orders') }}
),

-- Join customers with their orders
customer_orders as (
    select
        -- Surrogate key: combination of customer and order
        {{ dbt_utils.generate_surrogate_key(['c.customer_id', 'so.sales_order_id']) }} as customer_order_key,
        
        -- Customer information
        c.customer_id,
        c.first_name,
        c.last_name,
        c.territory_id,
        c.store_id,
        
        -- Order information
        so.sales_order_id,
        so.order_date,
        so.due_date,
        so.ship_date,
        so.status,
        so.online_order_flag,
        so.sales_order_number,
        
        -- Derived fields: order channel
        case 
            when so.online_order_flag = 1 then 'Online'
            else 'Offline'
        end as order_channel,
        
        -- Derived fields: order status description
        case 
            when so.status = 1 then 'In Process'
            when so.status = 2 then 'Approved'
            when so.status = 3 then 'Backordered'
            when so.status = 4 then 'Rejected'
            when so.status = 5 then 'Shipped'
            when so.status = 6 then 'Cancelled'
            else 'Unknown'
        end as order_status_description,
        
        -- Derived fields: days to ship
        datediff(day, so.order_date, so.ship_date) as days_to_ship,
        
        -- Derived fields: days until due
        datediff(day, so.order_date, so.due_date) as days_until_due,
        
        -- Order line item details
        so.order_detail_id,
        so.product_id,
        so.order_qty,
        so.unit_price,
        so.unit_price_discount,
        so.line_total,
        
        -- Derived fields: discount percentage
        case 
            when so.unit_price > 0 
            then (so.unit_price_discount / so.unit_price) * 100
            else 0
        end as discount_percentage,
        
        -- Derived fields: net amount (after discount)
        so.line_total as net_amount,
        
        -- Derived fields: gross amount (before discount)
        so.unit_price * so.order_qty as gross_amount,
        
        -- Derived fields: discount amount
        so.unit_price_discount * so.order_qty as total_discount_amount
        
    from customers c
    inner join sales_orders so
        on c.customer_id = so.customer_id
    where so.order_date is not null
        and so.customer_id is not null
)

select * from customer_orders

