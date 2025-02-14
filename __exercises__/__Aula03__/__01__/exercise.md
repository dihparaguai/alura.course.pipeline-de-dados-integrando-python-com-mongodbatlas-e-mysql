    • Crie um banco de dados no MongoDB Atlas chamado “db_produtos” e dentro dele, crie uma coleção chamada “produtos”, e então execute as seguintes tarefas:
    
    1. Utilize a API pública 'https://labdados.com/produtos' para extrair dados de produtos. 
    2. Insira esses dados na coleção criada no MongoDB Atlas.
    3. Mostre todos os valores distintos na coluna "Categoria do Produto".
    4. Realize uma consulta para extrair todos os produtos da categoria "eletronicos".
    5. Utilize uma expressão regular para filtrar produtos cujo nome contenha a palavra "Smart".
    6. Atualize os produtos com a categoria "eletronicos" para a nova categoria "gadgets".
    7. Renomeie a coluna "Data da Compra" para "data_compra", "Produto" para "produto_nome" e "Categoria do Produto" para "categoria_produto".
    8. Ajuste o formato da data para "ano/mês/dia" usando Pandas (por exemplo, "2022/05/30"). 
    9. Exporte os dados transformados para um arquivo CSV chamado "produtos_transformados.csv" usando Pandas.
    10. Feche a conexão com o MongoDB.