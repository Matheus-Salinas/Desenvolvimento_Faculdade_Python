📌 Descrição

Este projeto tem como objetivo aprofundar o conhecimento em PySpark, aplicando técnicas de manipulação de dados como:

join()

union()

select()

filtro

Exclusão de colunas

Uso de funções de janelamento (Window Functions)

Os dados utilizados são do arquivo selecao_fifa.csv, que contém informações sobre jogadores de diversas seleções.

Além disso, o projeto inclui o envio dos dados transformados para o Google BigQuery, permitindo análises mais avançadas na nuvem.

🚀 Tecnologias Utilizadas

Python: Linguagem principal do projeto.

PySpark: Biblioteca para processamento de grandes volumes de dados distribuídos.

Google Cloud Platform (GCP): Plataforma na nuvem utilizada para armazenamento e análise de dados.

BigQuery: Data warehouse do GCP para análise dos dados.

Pandas: Biblioteca para manipulação de dados antes da exportação para o BigQuery.

Anaconda: Ambiente configurado para facilitar a instalação e gerenciamento de dependências.

📊 Resultados Esperados

Tabelas Criadas e Carregadas no BigQuery:

tabela_geral: Contém todas as informações da seleção FIFA processadas.

tabela_peso: Média, peso máximo e mínimo dos jogadores por seleção.

tabela_altura: Média, altura máxima e mínima por seleção.

tabela_americas: União dos dados das seleções da América do Sul e do Norte.

tabelas_joins: Variantes de inner join, left join, right join, full join e anti join entre as seleções do Brasil e Argentina.
