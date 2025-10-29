# data-engineering-project-first
Primeiro projeto de Engenharia de Dados

Este projeto visa capturar, armazenar e transformar dados estruturados. Posteriormente vamos disponibilizar essas informações para visualização. A ideia é que não fossem utilizadas bases de dados prontas da internet, então esta é uma solução personalizada e adaptada ao nosso objetivo.

Por isso precisei lidar com alguns desafios inerentes e dados reais, que serão abordados no decorrer deste documento.


Estrutura do projeto 1 - dados dimensionais e relacionais:

Problemas:

-Criar uma estrutura (arquiterura) de um ambiente de analitycs;
-Foco maior na estrutura e arquitetura.

-Criar um data lake e um data warehouse (lakehouse), seguindo a arquitetura XXX;
-Utilizar o duckdb como query single para acessar os dados do lakehouse;
-Coletar dados do sistema que o professor manda no desafio NovaDrive + Planilhas, simulando dados vindos de vários ambientes
-Armazenar estes dados na camada landing - no postgres;
-Fazer as transformações de dados entre a dimensional e relacional utilizando DBT (Data Build Tool) através do DuckDB;
-Disponibilizar o metabase como ferramenta de DataViz e criar um dashboard