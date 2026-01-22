📊 Projeto ETL – Dados CNAE (IBGE)
📌 Visão Geral

Este projeto de Engenharia de Dados implementa um pipeline ETL (Extract, Transform, Load) em Python, utilizando dados públicos da API do IBGE, com foco na classificação CNAE. O objetivo é realizar a ingestão, transformação, normalização e persistência dos dados, garantindo qualidade, organização e integridade para uso analítico.

O pipeline foi desenvolvido seguindo boas práticas de modelagem relacional, evitando redundâncias e facilitando consultas futuras. Além disso, o processo é automatizado, permitindo execuções periódicas sem necessidade de intervenção manual.

🔄 Fluxo do Pipeline ETL

Extração

Consumo de dados da API REST do IBGE

Coleta das informações relacionadas à classificação CNAE

Transformação

Tratamento e limpeza dos dados

Padronização de campos

Normalização relacional para evitar redundâncias

Preparação dos dados para persistência

Carga

Armazenamento dos dados em SQL Server

Uso do SQLAlchemy para integração com o banco de dados

Garantia de integridade e organização das tabelas

Automação

Execução automatizada do pipeline utilizando a biblioteca schedule

Possibilidade de execuções periódicas e atualizações contínuas dos dados

🧠 Conceitos Aplicados

ETL (Extract, Transform, Load)

Ingestão de dados via APIs REST

Modelagem relacional

Normalização de dados

Automação de pipelines de dados

Boas práticas de Engenharia de Dados

🛠️ Tecnologias Utilizadas

Python

API REST (IBGE)

Pandas

SQL Server

SQLAlchemy

Schedule

Git / GitHub

🎯 Objetivo do Projeto

Demonstrar conhecimentos práticos em Engenharia de Dados, incluindo ingestão de dados externos, transformação e organização de dados, automação de pipelines e integração com bancos de dados relacionais. O projeto pode ser utilizado como base para análises, relatórios e evoluções futuras, como integração com ferramentas de visualização ou orquestração com Airflow.
