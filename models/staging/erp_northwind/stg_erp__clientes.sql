with 

fonte_clientes as (

    select 
    cast(customer_id as string) as id_cliente
    , cast(company_name as string) as nome_cliente
    , cast(contact_name as string) as nome_contato
    , cast(contact_title as string) as cargo_cliente
    , cast(city as string) as cidade_cliente
    , cast( case
         when region IS NULL then "ND"
         else region
         end as string) as regiao_cliente
    , cast(country as string) as pais_cliente
    , cast(phone as string) as contato_cliente
    from {{ source('erp', 'customers') }}
    
)

select *
from fonte_clientes