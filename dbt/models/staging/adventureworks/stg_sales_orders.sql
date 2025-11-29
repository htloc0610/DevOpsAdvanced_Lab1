{{
    config(
        materialized='view'
    )
}}

with sales_order_header_source as (
    select * from {{ source('adventureworks', 'SalesOrderHeader') }}
),

sales_order_detail_source as (
    select * from {{ source('adventureworks', 'SalesOrderDetail') }}
),

header_cleaned as (
    select
        -- Primary key
        cast(SalesOrderID as int) as sales_order_id,
        
        -- Dates (cast to date)
        cast(OrderDate as date) as order_date,
        cast(DueDate as date) as due_date,
        cast(ShipDate as date) as ship_date,
        
        -- Status and flags
        cast(Status as tinyint) as status,
        cast(OnlineOrderFlag as bit) as online_order_flag,
        
        -- Order numbers (cleaned)
        trim(upper(SalesOrderNumber)) as sales_order_number,
        trim(upper(PurchaseOrderNumber)) as purchase_order_number,
        
        -- Foreign keys
        cast(CustomerID as int) as customer_id,
        cast(SalesPersonID as int) as sales_person_id,
        cast(TerritoryID as int) as territory_id,
        
        -- Metadata
        cast(ModifiedDate as datetime) as last_modified_date
    from sales_order_header_source
    where SalesOrderID is not null
        and CustomerID is not null
),

detail_cleaned as (
    select
        -- Primary key
        cast(SalesOrderDetailID as int) as order_detail_id,
        
        -- Foreign keys
        cast(SalesOrderID as int) as sales_order_id,
        cast(ProductID as int) as product_id,
        
        -- Quantities and prices (cast and cleaned)
        cast(OrderQty as smallint) as order_qty,
        cast(UnitPrice as decimal(19, 4)) as unit_price,
        cast(UnitPriceDiscount as decimal(19, 4)) as unit_price_discount,
        cast(LineTotal as decimal(38, 6)) as line_total,
        
        -- Metadata
        cast(ModifiedDate as datetime) as last_modified_date
    from sales_order_detail_source
    where SalesOrderDetailID is not null
        and SalesOrderID is not null
        and ProductID is not null
)

select
    h.sales_order_id,
    h.order_date,
    h.due_date,
    h.ship_date,
    h.status,
    h.online_order_flag,
    h.sales_order_number,
    h.purchase_order_number,
    h.customer_id,
    h.sales_person_id,
    h.territory_id,
    d.order_detail_id,
    d.product_id,
    d.order_qty,
    d.unit_price,
    d.unit_price_discount,
    d.line_total,
    h.last_modified_date
from header_cleaned h
inner join detail_cleaned d
    on h.sales_order_id = d.sales_order_id

