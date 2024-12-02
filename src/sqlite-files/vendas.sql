
-- Criação da Tabela de VENDAS
-- Tive a liberdade de incluir taxanomia tb = objeto de tabela, 001 = código do objeto para sistema legado para tabela de vendas
CREATE TABLE IF NOT EXISTS tb001_vendas (
id  INTEGER PRIMARY KEY -- Identificador único da venda (Chave Primária)
, data_venda  DATE -- Data em que a venda foi realizada
, id_produto  INTEGER -- Identificador do produto vendido
, id_cliente  INTEGER -- Identificador do cliente que fez a compra
, quantidade  INTEGER -- Quantidade de itens vendidos
, valor_unitario  DECIMAL(10,2) -- Preço unitário do produto
, valor_total DECIMAL(10,2) -- Valor total da venda (quantidade * valor_unitário)
, id_vendedor INTEGER -- Identificador do vendedor responsável
, regiao  VARCHAR(50) -- Região onde a venda foi realizada
);
-- Limpeza dos registros na Tabela VENDAS antes do insert, considerando carga única
DELETE FROM tb001_vendas;
-- Insert de dados de amostra na tabela de VENDAS
INSERT INTO tb001_vendas (id, data_venda, id_produto, id_cliente, quantidade, valor_unitario, valor_total, id_vendedor, regiao) VALUES
(1, '2023-07-13', 108, 1820, 8, 50.64, 405.12, 2, 'Centro'),
(2, '2023-07-30', 104, 1290, 8, 35.21, 281.68, 18, 'Oeste'),
(3, '2023-09-13', 102, 1078, 4, 31.20, 124.80, 7, 'Centro'),
(4, '2023-05-15', 101, 1958, 8, 63.33, 506.64, 14, 'Leste'),
(5, '2023-03-22', 107, 1891, 1, 45.58, 45.58, 7, 'Leste'),
(6, '2023-02-20', 101, 1733, 6, 66.55, 399.30, 3, 'Oeste'),
(7, '2023-11-24', 106, 1115, 6, 70.34, 422.04, 19, 'Centro'),
(8, '2023-04-11', 109, 1340, 9, 49.69, 447.21, 12, 'Norte'),
(9, '2023-10-08', 104, 1412, 1, 68.71, 68.71, 11, 'Oeste'),
(10, '2023-02-02', 106, 1471, 3, 14.93, 44.79, 6, 'Oeste'),
(11, '2023-04-21', 105, 1968, 8, 33.50, 268.00, 4, 'Oeste'),
(12, '2023-01-13', 107, 1615, 10, 81.50, 815.00, 17, 'Leste'),
(13, '2023-09-03', 106, 1140, 3, 26.72, 80.16, 10, 'Sul'),
(14, '2023-04-30', 105, 1306, 5, 96.39, 481.95, 20, 'Centro'),
(15, '2023-01-21', 108, 1772, 5, 39.25, 196.25, 1, 'Norte'),
(16, '2023-05-31', 110, 1226, 7, 79.47, 556.29, 14, 'Oeste'),
(17, '2023-08-20', 104, 1668, 3, 49.43, 148.29, 13, 'Leste'),
(18, '2023-09-23', 105, 1879, 1, 61.15, 61.15, 16, 'Norte'),
(19, '2023-03-05', 101, 1895, 2, 27.39, 54.78, 8, 'Centro'),
(20, '2023-05-24', 109, 1608, 8, 36.92, 295.36, 9, 'Sul');

-- TESTE
-- select data_venda, sum(1) as qtd, sum(valor_total) as vl_total from tb001_vendas group by data_venda order by data_venda;