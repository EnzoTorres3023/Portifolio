USE restaurante;

-- Problema 1: Encontrar os pedidos de um funcionário específico que estão pendentes.
SELECT * FROM pedidos WHERE id_funcionario = 4 AND status = 'Pendente';

-- Problema 2: Listar todos os pedidos que ainda não foram para a conta do Papa.
SELECT * FROM pedidos WHERE status != 'Concluído';

-- Problema 3: Achar os pedidos de pratos específicos
SELECT * FROM pedidos WHERE id_produto IN (1, 3, 5, 7, 8);

-- Problema 4: Identificar clientes cujo nome começa com a letra 'C'.
SELECT * FROM clientes WHERE nome LIKE 'C%';

-- Problema 5: Verificar quais pratos usam 'carne' ou 'frango'.
SELECT * FROM info_produtos WHERE ingredientes LIKE '%carne%' OR ingredientes LIKE '%frango%';

-- Problema 6: Filtrar produtos em uma faixa de preço específica.
SELECT * FROM produtos WHERE preco BETWEEN 20 AND 30;

-- Problema 7: "Cancelar" um pedido, atualizando seu status para nulo.
UPDATE pedidos SET status = NULL WHERE id_pedido = 6;

-- Problema 8: Listar os pedidos que foram cancelados (status nulo).
SELECT * FROM pedidos WHERE status IS NULL;

-- Problema 9: Apresentar um relatório de status, tratando os nulos de forma amigável.
SELECT
    id_pedido,
    COALESCE(status, 'Cancelado') AS status_report
FROM
    pedidos;

-- Problema 10: Criar um relatório de análise salarial dos funcionários.
SELECT nome, cargo, salario,
    CASE
        WHEN salario > 3000 THEN 'Acima da média'
        ELSE 'Abaixo da média'
    END AS comparativo_salarial
FROM funcionarios;