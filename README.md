# Pipeline de Ingestão, Tratamento e Disponibilização de Dados de Ativos de Renda Variável

**Autor:** Bruno Oliveira Casotti 

## Descrição do Projeto

Este projeto visa criar uma solução para otimização de custos por meio de um pipeline de ingestão, tratamento e disponibilização de dados de ativos de renda variável, facilitando a coleta e análise de dados históricos e diários para investidores.

## Motivação

A queda na taxa básica de juros no Brasil em 2020 tornou os ativos de renda fixa menos atrativos, levando investidores a buscarem alternativas em renda variável. Este projeto foi desenvolvido para auxiliar startups como a App Wallit, oferecendo uma solução de gerenciamento de carteiras de investimentos a um custo acessível.

## Solução

A solução foi dividida em três partes, cada uma com objetivos e atividades específicas:

### Parte 1: Coleta de Dados

1. Coleta de dados cadastrais e de cotações históricas dos ativos.
2. Armazenamento dos dados em um Data Storage no formato Apache Parquet.

![processo de ingestao de dados drawio](https://github.com/BrunoCasotti/projeto_cotacao_renda_variavel/assets/56372315/0ac172dc-dc69-495d-af6d-71e8d15ec831)

### Parte 2: Tratamento dos Dados

1. Limpeza e padronização dos dados utilizando PySpark.
2. Armazenamento dos dados tratados no Data Warehouse.

![processo de tratamento](https://github.com/BrunoCasotti/projeto_cotacao_renda_variavel/assets/56372315/5239ce20-bd68-40ad-a203-81ae01e5c746)

### Parte 3: Disponibilização dos Dados

1. Configuração de consultas SQL para os dados tratados.
2. Implementação de uma interface de acesso aos dados.

![processo de disponibilizacao drawio](https://github.com/BrunoCasotti/projeto_cotacao_renda_variavel/assets/56372315/3a2e9619-dbc3-46e9-aa18-d952c2dfed30)

## Apresentação dos dados

A figura abaixo demonstra a utilização dos dados coletados e tratados durante o projeto em um dashboard criado no Power Bi. Na imagem, é possível identificar que foi selecionado o ativo “ITSA4” e abaixo são apresentados alguns dados referentes às cotações históricas e do último pregão do ativo, bem como os dados de cadastro da empresa ao qual o ticker se refere.

![dash projeto aplicado_pages-to-jpg-0001](https://github.com/BrunoCasotti/projeto_cotacao_renda_variavel/assets/56372315/f7293b96-f5cd-4caf-90c0-2fe674bb5ce5)

## Tecnologias Utilizadas

- **Linguagens:** Python e SQL
- **Ambiente de Desenvolvimento:** Databricks (Hospedado na Azure)
- **Biblioteca de Big Data:** PySpark
- **Armazenamento de Dados:** Apache Parquet, Data Storage em Cloud

## Benefícios e Justificativas

- **Redução de Custos:** Utilização de dados públicos e infraestrutura em cloud para diminuir despesas operacionais.
- **Acessibilidade:** Solução escalável e de baixo custo para startups financeiras.
- **Eficiência:** Processo otimizado de coleta e tratamento de dados, permitindo consultas rápidas e eficazes.

## Contato

Para mais informações, entre em contato com por e-mail [Bruno Oliveira Casotti](mailto:bruno.casotti@live.com) ou via [LinkedIn](https://www.linkedin.com/in/bruno-oliveira-casotti-573063193/).
