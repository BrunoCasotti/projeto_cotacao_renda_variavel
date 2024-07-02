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

### Parte 2: Tratamento dos Dados

1. Limpeza e padronização dos dados utilizando PySpark.
2. Armazenamento dos dados tratados no Data Warehouse.

### Parte 3: Disponibilização dos Dados

1. Configuração de consultas SQL para os dados tratados.
2. Implementação de uma interface de acesso aos dados.

## Tecnologias Utilizadas

- **Linguagem de Programação:** Python
- **Ambiente de Desenvolvimento:** Databricks
- **Biblioteca de Big Data:** PySpark
- **Armazenamento de Dados:** Apache Parquet, Data Storage em Cloud

## Benefícios e Justificativas

- **Redução de Custos:** Utilização de dados públicos e infraestrutura em cloud para diminuir despesas operacionais.
- **Acessibilidade:** Solução escalável e de baixo custo para startups financeiras.
- **Eficiência:** Processo otimizado de coleta e tratamento de dados, permitindo consultas rápidas e eficazes.

## Contato

Para mais informações, entre em contato com [Bruno Oliveira Casotti](mailto:bruno.casotti@live.com).
