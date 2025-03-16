1. Configure uma conexão com o MySQL usando Python.
2. Crie um banco de dados chamado db_produtos.
3. No banco de dados db_produtos, crie uma tabela chamada produtos com os seguintes campos: 
    1. id_produto (VARCHAR(50), chave primária)
    2. nome_produto (VARCHAR(100))
    3. categoria (VARCHAR(50))
    4. preco (DECIMAL(10, 2))
    5. quantidade_em_estoque (INT)
    6. data_adicao (DATE)
    7. ativo (BOOLEAN)
4. Leia um arquivo CSV chamado produtos.csv.
5. Insira os dados na tabela produtos utilizando placeholders.
6. Após a importação, consulte a tabela produtos e verifique se todos os dados foram inseridos corretamente.
