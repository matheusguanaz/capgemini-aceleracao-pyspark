# Capgemini - Aceleração PySpark 2022

Este projeto é parte do Programa de Aceleração [PySpark](https://spark.apache.org) da [Capgemini Brasil](https://www.capgemini.com/br-pt).
[<img src="https://www.capgemini.com/wp-content/themes/capgemini-komposite/assets/images/logo.svg" align="right" width="140">](https://www.capgemini.com/br-pt)

## Sobre

Este projeto consiste em realizar tarefas que buscam garantir a qualidade dos dados para responder perguntas de negócio a fim de gerar relatórios de forma assertiva. As tarefas são essencialmente apontar inconsistências nos dados originais, e realizar transformações que permitam tratar as inconsistências e enriquecer os dados. Em resumo, o projeto está organizado em três módulos: (1) qualidade, (2) transformação, e (3) relatório.

## Dependências

Para executar os Jupyter Notebooks deste repositório é necessário ter o [Spark instalado localmente](https://spark.apache.org/downloads.html) e também as seguintes dependências:

`pip install pyspark findspark`

## Estrutura de diretórios

```
├── LICENSE
├── README.md
├── data                    <- Diretório contendo os dados brutos.
│   ├── census-income
│   │   ├── census-income.csv
│   │   ├── census-income.names     <- Contém informações sobre os dados
│   ├── communities-crime
│   │   ├── communities-crime.csv
│   │   ├── communities-crime.names <- Contém informações sobre os dados
│   ├── online-retail
│   │   ├── online-retail.csv
│   │   ├── online-retail.names     <- Contém informações sobre os dados
│   ├── airports.csv
│   ├── planes.csv
│   ├── flights.csv
├── notebooks
│   ├── 1_quality.ipynb          <- Contém apontamentos de dados inconsistêntes.
│   ├── 2_transformation.ipynb   <- Contem tratamentos dos dados.
│   ├── 3_report.ipynb           <- Contém respostas de negócio baseadas em dados.
│
├── scripts
│   ├── README.md
│   ├── census-income.py       <- Contém respostas de negócio baseadas em dados
│   ├── communities-crime.py   <- Contém respostas de negócio baseadas em dados
│   ├── online-retail.py       <- Contém respostas de negócio baseadas em dados
```
