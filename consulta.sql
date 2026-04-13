USE restaurante;

-- 1. Selecionando os produtos (preço maior que R$ 30)
SELECT
    nome, categoria FROM produtos WHERE preco > 30;

-- 2. Encontrando os clientes (nascidos antes de 1985)
SELECT
    nome, telefone, data_nascimento FROM clientes WHERE YEAR(data_nascimento) < 1985;

-- 3. Procurando ingredientes específicos.
SELECT id_produto, ingredientes FROM info_produtos WHERE ingredientes LIKE '%carne%';

-- 4. Organizando o cardápio
SELECT nome, categoria FROM produtos ORDER BY categoria ASC, nome ASC;

-- 5. Exibindo o "Top 5" pratos mais caros
SELECT nome, preco FROM produtos ORDER BY preco DESC LIMIT 5;

-- 6. Selecionando 2 pratos principais, pulando os 5 primeiros.
SELECT nome, preco, categoria FROM produtos WHERE categoria = 'Prato Principal' ORDER BY id_produto LIMIT 2 OFFSET 5;

-- 7. Operação de Backup
CREATE TABLE if not exists backup_pedidos AS SELECT * FROM pedidos;