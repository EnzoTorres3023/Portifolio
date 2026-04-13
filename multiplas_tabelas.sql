USE restaurante;

-- 1. Unindo informações do produto com seus ingredientes.
-- Aqui, um INNER JOIN é perfeito, pois só queremos produtos que tenham informações de ingredientes.
SELECT
    p.id_produto,
    p.nome,
    p.descricao,
    ip.ingredientes
FROM
    produtos AS p
INNER JOIN
    info_produtos AS ip ON p.id_produto = ip.id_produto;

-- 2. Cruzando dados dos pedidos com os dados dos clientes.
-- Queremos saber qual cliente fez qual pedido.
SELECT
    pe.id_pedido,
    pe.quantidade,
    pe.data_pedido,
    c.nome AS nome_cliente,
    c.email AS email_cliente
FROM
    pedidos AS pe
JOIN
    clientes AS c ON pe.id_cliente = c.id_cliente;

-- 3. Juntando 3 tabelas: Pedidos, Clientes e Funcionários.
-- Quem pediu, quem atendeu e qual foi o pedido.
SELECT
    pe.id_pedido,
    pe.quantidade,
    pe.data_pedido,
    c.nome AS nome_cliente,
    f.nome AS nome_funcionario
FROM
    pedidos AS pe
JOIN
    clientes AS c ON pe.id_cliente = c.id_cliente
JOIN
    funcionarios AS f ON pe.id_funcionario = f.id_funcionario;

-- 4. Juntando 4 tabelas para ter a visão total do pedido.
-- Quem pediu, quem atendeu, o que pediu e quanto custou.
SELECT
    pe.id_pedido,
    pe.data_pedido,
    c.nome AS nome_cliente,
    f.nome AS nome_funcionario,
    pr.nome AS nome_produto,
    pe.quantidade,
    pr.preco AS preco_unitario
FROM
    pedidos AS pe
JOIN
    clientes AS c ON pe.id_cliente = c.id_cliente
JOIN
    funcionarios AS f ON pe.id_funcionario = f.id_funcionario
JOIN
    produtos AS pr ON pe.id_produto = pr.id_produto;

-- 5. Clientes com pedidos pendentes, ordenados do mais recente para o mais antigo.
-- Um JOIN para conectar as tabelas e um WHERE para filtrar o status.
SELECT
    c.nome AS nome_cliente,
    pe.id_pedido,
    pe.status
FROM
    clientes AS c
JOIN
    pedidos AS pe ON c.id_cliente = pe.id_cliente
WHERE
    pe.status = 'Pendente'
ORDER BY
    pe.id_pedido DESC;

-- 6. Encontrando clientes que nunca fizeram um pedido
-- A abordagem clássica e eficiente com LEFT JOIN.
SELECT
    c.nome,
    c.email
FROM
    clientes AS c
LEFT JOIN
    pedidos AS pe ON c.id_cliente = pe.id_cliente
WHERE
    pe.id_pedido IS NULL;

-- 7. Contando o total de pedidos por cliente.
-- Agrupamos por cliente e contamos seus pedidos.
SELECT
    c.nome AS nome_cliente,
    COUNT(pe.id_pedido) AS total_de_pedidos
FROM
    clientes AS c
JOIN
    pedidos AS pe ON c.id_cliente = pe.id_cliente
GROUP BY
    c.id_cliente
ORDER BY
    total_de_pedidos DESC;

-- 8. Calculando o valor total de cada pedido.
-- Multiplicamos a quantidade pelo preço unitário do produto.
SELECT
    pe.id_pedido,
    c.nome as nome_cliente,
    (pe.quantidade * pr.preco) AS valor_total_pedido
FROM
    pedidos AS pe
JOIN
    produtos AS pr ON pe.id_produto = pr.id_produto
JOIN
    clientes AS c ON pe.id_cliente = c.id_cliente
ORDER BY
    valor_total_pedido DESC;