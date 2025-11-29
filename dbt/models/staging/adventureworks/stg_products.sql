{{
    config(
        materialized='view'
    )
}}

with product_source as (
    select * from {{ source('adventureworks_production', 'Product') }}
),

subcategory_source as (
    select * from {{ source('adventureworks_production', 'ProductSubcategory') }}
),

cleaned as (
    select
        -- Primary key
        cast(p.ProductID as int) as product_id,
        
        -- Product information (cleaned)
        trim(upper(p.Name)) as product_name,
        trim(upper(p.ProductNumber)) as product_number,
        trim(upper(p.Color)) as color,
        trim(upper(p.Size)) as size,
        trim(upper(p.SizeUnitMeasureCode)) as size_unit_measure_code,
        trim(upper(p.WeightUnitMeasureCode)) as weight_unit_measure_code,
        
        -- Flags (normalized to boolean-like)
        cast(p.MakeFlag as bit) as make_flag,
        cast(p.FinishedGoodsFlag as bit) as finished_goods_flag,
        
        -- Numeric fields (cast and cleaned)
        cast(p.SafetyStockLevel as int) as safety_stock_level,
        cast(p.ReorderPoint as int) as reorder_point,
        cast(p.StandardCost as decimal(19, 4)) as standard_cost,
        cast(p.ListPrice as decimal(19, 4)) as list_price,
        cast(p.Weight as decimal(8, 2)) as weight,
        cast(p.DaysToManufacture as int) as days_to_manufacture,
        
        -- Categorical fields (normalized)
        trim(upper(p.ProductLine)) as product_line,
        trim(upper(p.Class)) as product_class,
        trim(upper(p.Style)) as product_style,
        
        -- Foreign keys
        cast(p.ProductSubcategoryID as int) as product_subcategory_id,
        cast(p.ProductModelID as int) as product_model_id,
        
        -- Dates
        cast(p.SellStartDate as date) as sell_start_date,
        cast(p.SellEndDate as date) as sell_end_date,
        cast(p.DiscontinuedDate as date) as discontinued_date,
        
        -- Subcategory information
        trim(upper(ps.Name)) as subcategory_name,
        cast(ps.ProductCategoryID as int) as product_category_id,
        
        -- Metadata
        cast(p.ModifiedDate as datetime) as last_modified_date
    from product_source p
    left join subcategory_source ps
        on p.ProductSubcategoryID = ps.ProductSubcategoryID
    where p.ProductID is not null
)

select * from cleaned

