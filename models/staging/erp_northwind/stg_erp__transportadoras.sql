select
    cast(shipper_id as int) as id_transportadora
    , cast(company_name as string) as nome_transportadora
    , cast(phone as string) as contato_transportadora
from {{ source('erp', 'shippers') }}