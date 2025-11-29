{{
    config(
        materialized='table'
    )
}}

with customer_orders as (
    select * from {{ ref('int_customer_orders') }}
),

-- Aggregate customer metrics
customer_metrics as (
    select
        -- Customer identifier
        customer_id,
        first_name,
        last_name,
        territory_id,
        store_id,
        
        -- Order count metrics
        count(distinct sales_order_id) as total_orders,
        count(distinct order_detail_id) as total_order_items,
        count(distinct case when order_channel = 'Online' then sales_order_id end) as online_orders,
        count(distinct case when order_channel = 'Offline' then sales_order_id end) as offline_orders,
        
        -- Date metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        datediff(day, min(order_date), max(order_date)) as customer_lifetime_days,
        
        -- Revenue metrics
        sum(line_total) as total_revenue,
        sum(gross_amount) as total_gross_revenue,
        sum(total_discount_amount) as total_discounts,
        avg(line_total) as avg_order_value,
        sum(line_total) / nullif(count(distinct sales_order_id), 0) as avg_order_amount,
        
        -- Quantity metrics
        sum(order_qty) as total_quantity_ordered,
        avg(order_qty) as avg_quantity_per_item,
        
        -- Discount metrics
        avg(discount_percentage) as avg_discount_percentage,
        sum(case when discount_percentage > 0 then 1 else 0 end) as items_with_discount,
        
        -- Shipping metrics
        avg(days_to_ship) as avg_days_to_ship,
        max(days_to_ship) as max_days_to_ship,
        min(days_to_ship) as min_days_to_ship,
        
        -- Order status metrics
        count(distinct case when order_status_description = 'Shipped' then sales_order_id end) as shipped_orders,
        count(distinct case when order_status_description = 'Cancelled' then sales_order_id end) as cancelled_orders,
        count(distinct case when order_status_description = 'In Process' then sales_order_id end) as in_process_orders,
        
        -- Product diversity
        count(distinct product_id) as unique_products_purchased
        
    from customer_orders
    group by 
        customer_id,
        first_name,
        last_name,
        territory_id,
        store_id
)

select * from customer_metrics

