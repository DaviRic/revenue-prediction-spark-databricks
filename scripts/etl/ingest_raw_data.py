from pyspark.sql import SparkSession # type: ignore
import findspark # type: ignore
findspark.init()
import pandas as pd # type: ignore

spark = SparkSession.builder.appName("Rossmann Store Sales").getOrCreate()

# Caminho dos arquivos
train_path = "/home/davicruvel/revenue-prediction-spark-databricks/data/raw/rossmann_store_sales/train.csv"
store_parh = "/home/davicruvel/revenue-prediction-spark-databricks/data/raw/rossmann_store_sales/store.csv"

# Lê o arquivo de vendas (treinamento)
train_df = spark.read.csv(train_path, header=True, inferSchema=True)

# Lê os arquivos com os dados das lojas
store_df = spark.read.csv(store_parh, header=True, inferSchema=True)

# Mostrar o esquema para garantir que tudo foi lido corretamente
print("Schema - Train:")
train_df.printSchema()

print("Schema - Store")
store_df.printSchema()

# Mostra as primeiras linhas
print("Train sample")
train_df.show(5)

print("Store sample")
store_df.show(5)

# Faz o join entre dois DataFrames pela coluna "store" para uma visão combinada
joined_df = train_df.join(store_df, on="Store", how="left")

print("Joined sample")
joined_df.show(5)
