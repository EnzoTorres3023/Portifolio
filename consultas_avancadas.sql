USE restaurante;

-- //////////////////////////////////////////////////////////////////
-- PARTE 1: ABSTRAINDO A COMPLEXIDADE COM VIEWS
-- //////////////////////////////////////////////////////////////////

-- Criar uma VIEW para simplificar nossa consulta mais complexa.
-- Uma VIEW é como um "atalho" ou uma tabela virtual. Ela guarda uma consulta SELECT
-- e nos permite usá-la como se fosse uma tabela normal.
-- Benefícios: LEITURA (esconde o JOIN complexo) e MANUTENÇÃO (se o JOIN mudar, alteramos só a VIEW).
CREATE VIEW resumo_pedido AS
SELECT
    pe.id_pedido,
    pe.data_pedido,
    c.nome AS nome_cliente,
    c.email AS email_cliente,
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

-- Usar nossa nova VIEW para calcular o total de cada pedido.
-- consulta fica mais LIMPA e LEGÍVEL sem o JOIN gigante!
SELECT
    id_pedido,
    nome_cliente,
    (quantidade * preco_unitario) AS total_calculado
FROM
    resumo_pedido;

-- Melhorando a VIEW para já incluir o campo total.
-- Assim, nem o cálculo precisamos fazer mais nas nossas consultas.
-- Usamos CREATE OR REPLACE para atualizar a VIEW existente.
CREATE OR REPLACE VIEW resumo_pedido AS
SELECT
    pe.id_pedido,
    pe.data_pedido,
    c.nome AS nome_cliente,
    c.email AS email_cliente,
    f.nome AS nome_funcionario,
    pr.nome AS nome_produto,
    pe.quantidade,
    pr.preco AS preco_unitario,
    (pe.quantidade * pr.preco) AS total_pedido -- CAMPO NOVO ADICIONADO!
FROM
    pedidos AS pe
JOIN
    clientes AS c ON pe.id_cliente = c.id_cliente
JOIN
    funcionarios AS f ON pe.id_funcionario = f.id_funcionario
JOIN
    produtos AS pr ON pe.id_produto = pr.id_produto;

-- Repetindo a consulta de forma simples.
SELECT
    id_pedido,
    nome_cliente,
    total_pedido -- Usando o campo que já vem pronto da VIEW.
FROM
    resumo_pedido;

-- Usando o EXPLAIN para espiar por baixo do capô.
-- O EXPLAIN nos mostra o que o banco de dados FAZ de verdade.
-- Ele revela que, apesar da nossa consulta simples, os JOINs ainda estão acontecendo.
-- Isso afeta a PERFORMANCE, mas ganhamos em LEITURA e MANUTENÇÃO.
EXPLAIN SELECT id_pedido, nome_cliente, total_pedido FROM resumo_pedido;

-- //////////////////////////////////////////////////////////////////
-- PARTE 2: CRIANDO FERRAMENTAS REUTILIZÁVEIS COM FUNCTIONS
-- //////////////////////////////////////////////////////////////////
-- Para criar functions, precisamos mudar o delimitador padrão (;) para o MySQL não se confundir.
DELIMITER $$

-- Criar uma função que busca ingredientes.
-- Uma FUNCTION é um código que guardamos para usar depois.
-- recebe entrada e retorna um valor.
-- Benefícios: REUTILIZAÇÃO e ABSTRAÇÃO da lógica.
CREATE FUNCTION BuscaIngredientesProduto(id_prod INT)
RETURNS TEXT
DETERMINISTIC
BEGIN
    DECLARE ingredientes_encontrados TEXT;
    SELECT ingredientes INTO ingredientes_encontrados FROM info_produtos WHERE id_produto = id_prod;
    RETURN ingredientes_encontrados;
END$$

-- Executar a nossa nova função.
-- Agora, em qualquer lugar, podemos chamar essa ferramenta que criamos.
SELECT BuscaIngredientesProduto(10) AS ingredientes_do_risoto$$


-- Criar uma função de análise mais complexa.
-- Esta função compara o total de um pedido com a média de TODOS os pedidos.
CREATE FUNCTION mediaPedido(id_ped INT)
RETURNS VARCHAR(50)
DETERMINISTIC
BEGIN
    DECLARE total_do_pedido DECIMAL(10,2);
    DECLARE media_de_todos DECIMAL(10,2);
    DECLARE resultado VARCHAR(50);

    -- Calcula o total do pedido específico que foi passado como argumento
    SELECT total_pedido INTO total_do_pedido FROM resumo_pedido WHERE id_pedido = id_ped;

    -- Calcula a média do total de TODOS os pedidos
    SELECT AVG(total_pedido) INTO media_de_todos FROM resumo_pedido;

    -- Compara e define a mensagem de retorno
    IF total_do_pedido > media_de_todos THEN
        SET resultado = 'Acima da média';
    ELSEIF total_do_pedido < media_de_todos THEN
        SET resultado = 'Abaixo da média';
    ELSE
        SET resultado = 'Na média';
    END IF;

    RETURN resultado;
END$$

-- Testando nossa função de análise com dois pedidos diferentes.
SELECT mediaPedido(5) AS analise_pedido_5$$
SELECT mediaPedido(6) AS analise_pedido_6$$

-- Redefinir o delimitador padrão
DELIMITER ;