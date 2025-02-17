    • Configure uma conexão com o MySQL usando Python.
    • Crie um banco de dados chamado db_clientes.
    • No banco de dados db_clientes, crie uma tabela chamada clientes com os seguintes campos:
        ○  id (VARCHAR(50), chave primária)
        ○  nome (VARCHAR(100))
        ○  idade (TINYINT)
        ○  email (VARCHAR(100))
        ○  data_cadastro (DATE)
        ○  ativo (BOOLEAN)
    • Leia um arquivo CSV chamado dataset.csv.
    • Insira os dados na tabela clientes utilizando placeholders.
    • Após a importação, consulte a tabela clientes e verifique se todos os dados foram inseridos corretamente.