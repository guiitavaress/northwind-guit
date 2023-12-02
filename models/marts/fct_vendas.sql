with
    funcionarios as (
        select *
        from {{ ref('dim_funcionarios') }}
    )

    , produtos as (
        select *
        from {{ ref('dim_produtos') }}
    )

    , int_vendas as (
        select *
        from {{ ref('int_vendas__pedido_itens') }}
    )

    , clientes as (
        select *
        from {{ ref('dim_clientes') }}
    )

    , transportadoras as (
        select *
        from {{ ref('dim_transportadoras') }}
    )

    , joined_tabelas as (
        select 
        int_vendas.sk_pedido_item
        , int_vendas.id_pedido
        , int_vendas.id_funcionario
        , int_vendas.id_cliente
        , int_vendas.id_transportadora
        , int_vendas.id_produto
        , int_vendas.data_do_pedido
        , int_vendas.data_do_envio
        , int_vendas.data_requerida_entrega
        , int_vendas.frete
        , int_vendas.destinatario
        , int_vendas.endereco_destinatario
        , int_vendas.cep_destinatario
        , int_vendas.cidade_destinatario
        , int_vendas.regiao_destinatario
        , int_vendas.pais_destinatario
        , int_vendas.desconto_perc
        , int_vendas.preco_da_unidade
        , int_vendas.quantidade
        , produtos.nome_produto
        , produtos.eh_descontinuado
        , produtos.nome_categoria
        , produtos.nome_fornecedor
        , produtos.pais_fornecedor
        , funcionarios.nome_funcionario
        , funcionarios.nome_gerente
        , funcionarios.cargo_funcionario
        , funcionarios.dt_nascimento_funcionario
        , funcionarios.dt_contratacao
        , clientes.nome_cliente
        , clientes.nome_contato
        , clientes.cargo_cliente
        , clientes.cidade_cliente
        , clientes.regiao_cliente
        , clientes.pais_cliente
        , clientes.contato_cliente
        , transportadoras.nome_transportadora
        , transportadoras.contato_transportadora
        from int_vendas
        left join produtos on
            int_vendas.id_produto = produtos.id_produto
        left join funcionarios on
            int_vendas.id_funcionario = funcionarios.id_funcionario
        left join clientes on
            int_vendas.id_cliente = clientes.id_cliente
        left join transportadoras on
            int_vendas.id_transportadora = transportadoras.id_transportadora
    )

    , transformacoes as (
        select
            *
            , quantidade * preco_da_unidade as total_bruto
            , quantidade * preco_da_unidade * (1 - desconto_perc) as total_liquido
            , case
                when desconto_perc > 0 then "Sim"
                else "No"
            end as teve_desconto
            , frete / count(id_pedido) over(partition by id_pedido) as frete_ponderado
        from joined_tabelas
    )

    , select_final as (
        select
            /* Chaves */
            sk_pedido_item
            , id_pedido
            , id_funcionario
            , id_cliente
            , id_transportadora
            , id_produto
            /* Datas */
            , data_do_pedido
            , data_do_envio
            , data_requerida_entrega
            /* Métricas */
            , desconto_perc
            , preco_da_unidade
            , quantidade
            , total_bruto
            , total_liquido
            , teve_desconto
            , frete_ponderado
            /* Categorias */
            , destinatario
            , endereco_destinatario
            , cep_destinatario
            , cidade_destinatario
            , regiao_destinatario
            , pais_destinatario
            , nome_produto
            , eh_descontinuado
            , nome_categoria
            , nome_fornecedor
            , pais_fornecedor
            , nome_funcionario
            , nome_gerente
            , cargo_funcionario
            , dt_nascimento_funcionario
            , dt_contratacao
            , nome_cliente
            , nome_contato
            , cargo_cliente
            , cidade_cliente
            , regiao_cliente
            , pais_cliente
            , contato_cliente
            , nome_transportadora
            , contato_transportadora
        from transformacoes
    )

select *
from select_final