{{
    config(
        materialized='table'
    )
}}

with product_sales as (
    select * from {{ ref('int_product_sales') }}
),

-- Aggregate product performance metrics
product_performance as (
    select
        -- Product identifier
        product_id,
        product_name,
        product_number,
        color,
        size,
        product_line,
        product_class,
        product_style,
        product_subcategory_id,
        subcategory_name,
        product_category_id,
        list_price,
        standard_cost,
        make_flag,
        finished_goods_flag,
        product_availability_status,
        is_current_product,
        
        -- Sales count metrics
        count(distinct sales_order_id) as total_orders,
        count(distinct order_detail_id) as total_sales_items,
        
        -- Date metrics
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        datediff(day, min(order_date), max(order_date)) as sales_period_days,
        
        -- Revenue metrics
        sum(line_total) as total_revenue,
        sum(unit_price * order_qty) as total_gross_revenue,
        sum(unit_price_discount * order_qty) as total_discounts,
        avg(unit_price) as avg_selling_price,
        min(unit_price) as min_selling_price,
        max(unit_price) as max_selling_price,
        
        -- Quantity metrics
        sum(order_qty) as total_quantity_sold,
        avg(order_qty) as avg_quantity_per_order,
        min(order_qty) as min_quantity_sold,
        max(order_qty) as max_quantity_sold,
        
        -- Profit metrics
        sum(profit_amount) as total_profit,
        avg(profit_margin_percentage) as avg_profit_margin_percentage,
        min(profit_margin_percentage) as min_profit_margin_percentage,
        max(profit_margin_percentage) as max_profit_margin_percentage,
        
        -- Price metrics
        avg(price_difference_from_list) as avg_price_difference_from_list,
        avg(price_ratio_to_list) as avg_price_ratio_to_list,
        avg(discount_percentage) as avg_discount_percentage,
        
        -- Performance ratios
        case 
            when list_price > 0 
            then avg(unit_price) / list_price 
            else null 
        end as avg_price_to_list_ratio,
        
        -- Revenue per unit
        sum(line_total) / nullif(sum(order_qty), 0) as revenue_per_unit,
        
        -- Profit per unit
        sum(profit_amount) / nullif(sum(order_qty), 0) as profit_per_unit
        
    from product_sales
    group by 
        product_id,
        product_name,
        product_number,
        color,
        size,
        product_line,
        product_class,
        product_style,
        product_subcategory_id,
        subcategory_name,
        product_category_id,
        list_price,
        standard_cost,
        make_flag,
        finished_goods_flag,
        product_availability_status,
        is_current_product
)

select * from product_performance

