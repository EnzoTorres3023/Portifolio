USE restaurante;

-- 1. Qual o volume total de pedidos do restaurante?
SELECT COUNT(id_pedido) AS total_de_pedidos
FROM pedidos;

-- 2. Quantos clientes diferentes já compraram conosco?
SELECT COUNT(DISTINCT id_cliente) AS total_clientes_compradores
FROM pedidos;

-- 3. Qual a média de preço do nosso cardápio?
SELECT AVG(preco) AS media_precos_cardapio
FROM produtos;

-- 4. Qual a faixa de preço dos nossos produtos? (Do mais barato ao mais caro)
SELECT
    MIN(preco) AS produto_mais_barato,
    MAX(preco) AS produto_mais_caro
FROM produtos;

-- 5. Qual o nosso "Top 5" de produtos mais caros? (Ranking)
SELECT
    nome,
    preco,
    RANK() OVER (ORDER BY preco DESC) AS ranking_de_preco
FROM produtos
LIMIT 5;

-- 6. Qual a média de preço por cada categoria de produto?
SELECT
    categoria,
    AVG(preco) AS media_de_preco_por_categoria
FROM produtos
GROUP BY categoria;

-- 7. Quantos produtos recebemos de cada fornecedor?
SELECT
    fornecedor,
    COUNT(id_produto) AS quantidade_de_produtos
FROM info_produtos
GROUP BY fornecedor;

-- 8. Quais fornecedores são nossos parceiros mais fortes? (Mais de 1 produto)
SELECT
    fornecedor,
    COUNT(id_produto) AS quantidade_de_produtos
FROM info_produtos
GROUP BY fornecedor
HAVING COUNT(id_produto) > 1;

-- 9. Quem são os clientes que vieram apenas uma vez?
SELECT
    c.nome,
    c.email,
    COUNT(p.id_pedido) AS numero_de_pedidos
FROM pedidos AS p
JOIN clientes AS c ON p.id_cliente = c.id_cliente
GROUP BY p.id_cliente
HAVING COUNT(p.id_pedido) = 1;