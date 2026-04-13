# --- ETAPA 0: IMPORTAÇÕES E SESSÃO SPARK ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, var_pop, min, first, last, countDistinct, format_number
from pyspark.sql.window import Window

# Iniciar a Sessão Spark
spark = SparkSession.builder \
    .appName("Agregacoes com Dados do YouTube") \
    .getOrCreate()

print(f"Sessão Spark iniciada. Versão: {spark.version}")

# --- ETAPA 1: LEITURA DOS DADOS ---

# 1. Leia o arquivo 'videos-preparados.snappy.parquet'
print("\n--- 1. Lendo o DataFrame preparado ---")
df_video = spark.read.parquet('videos-preparados-parquet/')
print("Arquivo 'videos-preparados-parquet' carregado.")
df_video.show(5)

# --- ETAPA 2: AGREGAÇÕES E ANÁLISES ---

# 2. Calcule a quantidade de registros para cada valor único da coluna "Keyword"
print("\n--- 2. Contagem de vídeos por Categoria (Keyword) ---")
df_video.groupBy("Keyword") \
    .count() \
    .orderBy(col("count").desc()) \
    .show(10)

# 3. Calcule a média da coluna "Interaction" para cada valor único da coluna 'Keyword'
print("\n--- 3. Média de Interações por Categoria ---")
df_video.groupBy("Keyword") \
    .agg(
        format_number(avg("Interaction"), 2).alias("Media_Interacoes")
    ) \
    .orderBy(col("Media_Interacoes").cast("float").desc()) \
    .show(10)

# 4. Calcule o valor máximo da coluna "Interaction" para cada 'Keyword'
print("\n--- 4. Vídeo com Maior Interação (Rank) por Categoria ---")
df_video.groupBy("Keyword") \
    .agg(
        max("Interaction").alias("Rank_Interactions")
    ) \
    .orderBy(col("Rank_Interactions").desc()) \
    .show(10)

# 5. Calcule a média e a variância da coluna 'Views' para cada 'Keyword'
print("\n--- 5. Média e Variância de Visualizações por Categoria ---")
df_video.groupBy("Keyword") \
    .agg(
        format_number(avg("Views"), 2).alias("Media_Views"),
        format_number(var_pop("Views"), 2).alias("Variancia_Views")
    ) \
    .orderBy(col("Media_Views").cast("float").desc()) \
    .show(10)

# 6. Calcule a média, o valor mínimo e o valor máximo de 'Views' sem casas decimais
print("\n--- 6. Estatísticas de Visualizações (sem decimais) por Categoria ---")
df_video.groupBy("Keyword") \
    .agg(
        avg("Views").cast("integer").alias("Media_Views_Int"),
        min("Views").alias("Min_Views"),
        max("Views").alias("Max_Views")
    ) \
    .orderBy("Max_Views", ascending=False) \
    .show(10)

# 7. Mostre o primeiro e o último 'Published At' para cada 'Keyword'
# Usamos min() e max() para garantir que pegamos a data mais antiga e a mais nova.
print("\n--- 7. Janela de Publicação (Data Mais Antiga e Mais Nova) por Categoria ---")
df_video.groupBy("Keyword") \
    .agg(
        min("Published At").alias("Primeira_Publicacao"),
        max("Published At").alias("Ultima_Publicacao")
    ) \
    .orderBy("Ultima_Publicacao", ascending=False) \
    .show(10)

# 8. Conte todos os 'title' de forma normal e todos os únicos
print("\n--- 8. Contagem de Títulos Totais vs. Títulos Únicos ---")
df_video.select(
    count("Title").alias("Total_Titulos"),
    countDistinct("Title").alias("Titulos_Unicos")
).show()

# 9. Mostre a quantidade de registros ordenados por ano em ordem ascendente
print("\n--- 9. Quantidade de Vídeos por Ano ---")
df_video.groupBy("Year") \
    .count() \
    .orderBy("Year", ascending=True) \
    .show()

# 10. Mostre a quantidade de registros ordenados por ano e mês
print("\n--- 10. Quantidade de Vídeos por Ano e Mês ---")
df_video.groupBy("Year", "Month") \
    .count() \
    .orderBy("Year", "Month", ascending=True) \
    .show()

# 11. Calcule a média acumulativa de ‘Likes’ para cada ‘Keyword’ ao longo dos anos
print("\n--- 11. Média Acumulativa de Likes por Categoria ao Longo dos Anos ---")
# Definindo a "janela" de operação: para cada Keyword, ordenada por Ano
windowSpec = Window.partitionBy("Keyword").orderBy("Year").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Criando a coluna com a média sobre a janela
df_acumulado = df_video.withColumn(
    "Media_Acumulativa_Likes",
    avg("Likes").over(windowSpec)
)

df_acumulado.select("Keyword", "Year", "Likes", "Media_Acumulativa_Likes") \
    .filter(col("Keyword") == 'tech') \
    .orderBy("Year") \
    .show(20)

# --- FINALIZAÇÃO ---
print("\n--- Tarefa de agregações concluída! ---")
spark.stop()
