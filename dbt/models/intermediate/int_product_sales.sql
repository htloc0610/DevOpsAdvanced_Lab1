{{
    config(
        materialized='table'
    )
}}

with products as (
    select * from {{ ref('stg_products') }}
),

sales_orders as (
    select * from {{ ref('stg_sales_orders') }}
),

-- Join products with their sales
product_sales as (
    select
        -- Surrogate key: combination of product and order detail
        {{ dbt_utils.generate_surrogate_key(['p.product_id', 'so.order_detail_id']) }} as product_sale_key,
        
        -- Product information
        p.product_id,
        p.product_name,
        p.product_number,
        p.color,
        p.size,
        p.product_line,
        p.product_class,
        p.product_style,
        p.product_subcategory_id,
        p.subcategory_name,
        p.product_category_id,
        p.list_price,
        p.standard_cost,
        p.make_flag,
        p.finished_goods_flag,
        
        -- Order information
        so.sales_order_id,
        so.order_date,
        so.order_detail_id,
        so.order_qty,
        so.unit_price,
        so.unit_price_discount,
        so.line_total,
        
        -- Derived fields: profit margin
        case 
            when p.standard_cost > 0 
            then ((so.unit_price - p.standard_cost) / p.standard_cost) * 100
            else null
        end as profit_margin_percentage,
        
        -- Derived fields: profit amount
        (so.unit_price - p.standard_cost) * so.order_qty as profit_amount,
        
        -- Derived fields: discount percentage
        case 
            when so.unit_price > 0 
            then (so.unit_price_discount / so.unit_price) * 100
            else 0
        end as discount_percentage,
        
        -- Derived fields: price difference from list price
        so.unit_price - p.list_price as price_difference_from_list,
        
        -- Derived fields: price ratio to list price
        case 
            when p.list_price > 0 
            then so.unit_price / p.list_price
            else null
        end as price_ratio_to_list,
        
        -- Derived fields: product availability status
        case 
            when p.discontinued_date is not null then 'Discontinued'
            when p.sell_end_date is not null and p.sell_end_date < getdate() then 'Ended'
            when p.sell_start_date > getdate() then 'Not Yet Available'
            else 'Available'
        end as product_availability_status,
        
        -- Derived fields: is current product
        case 
            when p.discontinued_date is null 
                and (p.sell_end_date is null or p.sell_end_date >= getdate())
                and p.sell_start_date <= getdate()
            then 1
            else 0
        end as is_current_product
        
    from products p
    inner join sales_orders so
        on p.product_id = so.product_id
    where so.product_id is not null
        and p.product_id is not null
)

select * from product_sales

