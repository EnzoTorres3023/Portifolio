# --- ETAPA 0: IMPORTAÇÕES E SESSÃO SPARK ---
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Iniciar a Sessão Spark
spark = SparkSession.builder \
    .appName("Otimizacao de Joins com Dados do YouTube") \
    .getOrCreate()

print(f"Sessão Spark iniciada. Versão: {spark.version}")

# --- ETAPA 1: LEITURA DOS DADOS ---
# 1. Leia o arquivo 'videos-preparados.snappy.parquet'
print("\n--- Lendo o DataFrame 'df_video' ---")

df_video = spark.read.parquet('videos-preparados-parquet/')
print(f"df_video carregado com {df_video.count()} registros.")

# 2. Leia o arquivo 'video-comments-tratados.snappy.parquet'
print("\n--- Lendo o DataFrame 'df_comments' ---")
df_comments = spark.read.parquet('videos-comments-tratados-parquet/')
print(f"df_comments carregado com {df_comments.count()} registros.")

# --- ETAPA 2: JOIN PADRÃO COM SPARK SQL ---

# 3. Crie tabelas temporárias para ambos os dataframes
print("\n--- Criando tabelas temporárias ---")
df_video.createOrReplaceTempView("tabela_videos")
df_comments.createOrReplaceTempView("tabela_comments")
print("Tabelas 'tabela_videos' e 'tabela_comments' criadas.")

# 4. Faça um join das tabelas utilizando o spark.sql
print("\n--- Executando o JOIN padrão com Spark SQL ---")

join_video_comments_sql = spark.sql("""
    SELECT
        v.Title,
        v.Views,
        v.Likes,
        c.`Likes_Comment`,
        c.Sentiment
    FROM tabela_videos v
    INNER JOIN tabela_comments c ON v.`Video ID` = c.`Video ID`
""")
print("JOIN com SQL concluído. Contagem de registros:", join_video_comments_sql.count())


# --- ETAPA 3: JOIN COM REPARTITION E COALESCE ---

print("\n--- Repetindo o JOIN, agora com Repartition e Coalesce ---")
num_particoes = 200
df_video_repart = df_video.repartition(num_particoes, col("Video ID"))
df_comments_repart = df_comments.repartition(num_particoes, col("Video ID"))

print("DataFrames reparticionados pela chave de join.")

join_repart = df_video_repart.join(df_comments_repart, "Video ID", "inner")
print(f"JOIN com repartition concluído. Contagem de registros: {join_repart.count()}")

join_final_coalesce = join_repart.coalesce(4)
print(f"Número de partições após coalesce: {join_final_coalesce.rdd.getNumPartitions()}")


# --- ETAPA 4: ANÁLISE DOS PLANOS DE EXECUÇÃO ---

print("\n--- Analisando os planos de execução (Explain) ---")
 # join_video_comments_sql.explain(extended=True)
 # join_repart.explain(extended=True)


# --- ETAPA 5: O JOIN OTIMIZADO (AQUI ESTAVA O ERRO DE SINTAXE) ---

print("\n--- Criando o JOIN OTIMIZADO ---")

video_otimizado = df_video \
    .select("Video ID", "Title", "Views", "Likes") \
    .filter(col("Likes") > 10000)

comments_otimizado = df_comments \
    .select("Video ID", "Likes_Comment", "Sentiment") \
    .filter(col("Sentiment") == 1)

# Executando o join otimizado
join_otimizado = video_otimizado.join(comments_otimizado, "Video ID", "inner")

print("JOIN otimizado concluído. Contagem de registros:", join_otimizado.count())
join_otimizado.show(10)


# --- ETAPA 6: SALVANDO O RESULTADO FINAL ---

print("\n--- 8. Salvando o resultado otimizado ---")
try:
    join_otimizado.write.mode('overwrite').parquet('join-videos-comments-otimizado')
    print("Arquivo 'join-videos-comments-otimizado' salvo com sucesso!")
except Exception as e:
    print(f"Erro ao salvar: {e}")

# --- FINALIZAÇÃO ---
print("\n--- Tarefa de otimização concluída! ---")
spark.stop()