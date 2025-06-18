# 🚀 Data Pipeline de Vendas + Previsão de Receita com Databricks
Esse projeto desenvolve um pipeline completo de Machine Learning utilizando Apache Spark para prever a receita de vendas de loja.
O objetivo é demonstrar na prática como pipeline de dados podem ser estruturados para resolver problemas de negócios, desde a ingestão
e transformação dos dados até a modelagem, avaliação e análise dos resultados.

## Pipeline do Projeto
```
graph TD
A[Ingestão dos Dados] --> B[Limpeza dos Dados]
B --> C[Feature Engineering]
C --> D[Divisão dos Dados (Treino/Teste)]
D --> E[Treinamento do Modelo]
E --> F[Avaliação e Análise dos Resultados]
```

## 🔧 Ferramentas e Tecnologias
- Apache Spark (PySpark) - Processamento distribuído e Machine Learning
- Python - Análise e manipulação de dados
- Pandas e Matplotlib - Análise exploratória e visualização
- Spark MLlib - Machine Learning escalável
- Jupyter Notebook - Desenvolvimento e experimentação
- Git e GitHub - Versionamento e colaboração

## 📂 Estrutura do Repositório
```
📦 revenue-prediction-spark-databricks
├── 📁 data/                  # Dados brutos e processados
├── 📁 notebooks/             # Notebooks com cada etapa do pipeline
    ├── 01_exploracao.ipynb
    └── 02_processing.ipynb
├── 📁 docs/                  # Diagrama do pipeline e recursos visuais
├── README.md              # Documentação do projeto
└── requirements.txt       # Dependências do projeto
```

## 🏗️ Etapas do Pipeline
1. Ingestão dos dados
   - Leitura dos dados em CSV.
   - Criação de DataFrames Spark.

2. Limpeza dos dados
   - Tratamento de valores nulos.
   - Conversão de tipos e consistência.

3. Feature Engineering
   - Criação de variáveis como:
     - `IsPromo` -> booleano que indica se a loja está em promoção ou não.
     - `CompetitionOpenTime` -> tempo desde a abertura do concorrente.
   - Conversão de variáveis categóricas para numéricas.

4. Divisão dos dados
   - Dados separados em treino e teste (80/20).

5. Treinamento do Modelo
   - Algoritmo: Regressão Linear usando Spark MLlib.
   - Pipeline com `VectorAssembler` e modelo de regressão.

6. Avaliação e Análise dos Resultados
   - Métricar utilizadas:
     - RMSE;
     - MAE
     - R²
     - MAPE

  - Análise dos erros:
    - Distribuição dos erros absolutos.
    - Distribuição dos erros percentuais (APE).

  - Gráficos:
    - Dispersão entre `Sales` e `Prediction`
    - Histogramas dos Erros.

## Principais Resultados
- MAPE: 5,67% (bom nível de precisão)
- Mais de 87% das previsões apresentam erro percentual inferior a 10%
- Modelo adequado para cenários preditivos iniciais com possibilidade de melhorias

## Possíveis melhorias
- Implementação de modelos mais robustos:
  - Random Forest
  - Gradient Boosted Trees
- Integração com banco de dados ou data lakes

## Aprendizado
- Hands-on com pipelines de Machine Learning no Apache Spark.
- Entendimento do ciclo completo de projeto de dados:
  - Data Engineering -> Data Science -> Análise de Resultados.
- Aplicação de técnicas de Feature Engineering no Spark

