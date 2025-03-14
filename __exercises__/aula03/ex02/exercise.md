• Crie um banco de dados no MongoDB Atlas chamado "db_usuarios" e, dentro dele, crie uma coleção chamada "usuarios". Em seguida, execute as seguintes tarefas:

1. Utilize a API pública https://randomuser.me/api/?results=50 para extrair dados de usuários.
2. Insira esses dados na coleção criada no MongoDB Atlas.
3. Mostre todos os valores distintos da coluna "gender".
4. Realize uma consulta para extrair todos os usuários cuja nacionalidade ("nat") seja "US".
5. Utilize uma expressão regular para filtrar usuários cujo primeiro nome contenha a palavra "John".
6. Atualize os registros dos usuários do gênero "female" com um campo "status" com o valor "verified".
7. Renomeie as colunas:
    ○ "registered.date" para "data_registro",
    ○ "name.first" para "primeiro_nome",
    ○ "name.last" para "sobrenome". E
    ○ "gender" para "genero"
8. Ajuste o formato da data para "ano/mês/dia" usando Pandas (por exemplo, transformando "2017-03-16T17:07:19.142Z" em "2017/03/16").
9. Exporte os dados de gênero, primeiro nome e data de registro, transformados para um arquivo CSV chamado "usuarios_transformados.csv" usando Pandas.
10. Feche a conexão com o MongoDB.
