{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('adventureworks', 'Customer') }}
),

person_source as (
    select * from {{ source('adventureworks_person', 'Person') }}
),

cleaned as (
    select
        -- Primary key
        cast(c.CustomerID as int) as customer_id,
        
        -- Person information (cleaned)
        trim(upper(p.FirstName)) as first_name,
        trim(upper(p.LastName)) as last_name,
        trim(lower(p.EmailPromotion)) as email_promotion,
        
        -- Customer attributes
        cast(c.StoreID as int) as store_id,
        cast(c.TerritoryID as int) as territory_id,
        cast(c.PersonID as int) as person_id,
        
        -- Metadata
        cast(c.ModifiedDate as datetime) as last_modified_date
    from source c
    left join person_source p
        on c.PersonID = p.BusinessEntityID
    where c.CustomerID is not null
        and c.PersonID is not null
)

select * from cleaned

