# 1. Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, count

# Criar a Sessão Spark
spark = SparkSession.builder \
    .appName("Tratamento de Dados do YouTube") \
    .getOrCreate()

print(f"Sessão Spark iniciada. Versão: {spark.version}")

# --- ETAPA 1: LEITURA E LIMPEZA INICIAL ---

print("\n Lendo arquivos iniciais ")
# 1. Leia o arquivo ‘videos-stats.csv'
df_video = spark.read.csv('videos-stats.csv', header=True, inferSchema=True)
print("Arquivo 'videos-stats.csv' carregado.")

# 2. Altere os valores nulos dos campos 'Likes', 'Comments' e 'Views' para 0
df_video = df_video.fillna(0, subset=['Likes', 'Comments', 'Views'])
print("Valores nulos de Likes, Comments e Views preenchidos com 0.")

# 3. Leia o arquivo ‘comments.csv'
df_comentario = spark.read.csv('comments.csv', header=True, inferSchema=True)
print("Arquivo 'comments.csv' carregado.")

# 4. Calcule a quantidade de registros inicial
print("\n Contagem inicial de registros ")
print(f"df_video tinha {df_video.count()} registros.")
print(f"df_comentario tinha {df_comentario.count()} registros.")

# 5. Remova os registros com 'Video ID' nulos e recalcule
print("\n Removendo registros com 'Video ID' nulo ")
df_video = df_video.dropna(subset=['Video ID'])
df_comentario = df_comentario.dropna(subset=['Video ID'])
print(f"df_video agora tem {df_video.count()} registros.")
print(f"df_comentario agora tem {df_comentario.count()} registros.")

# 6. Remova os registros duplicados de df_video
print("\n Removendo 'Video ID' duplicados de df_video ")
registros_antes = df_video.count()
df_video = df_video.dropDuplicates(['Video ID'])
registros_depois = df_video.count()
print(f"{registros_antes - registros_depois} registros duplicados foram removidos. Total agora: {registros_depois}")

# --- ETAPA 2: TRANSFORMAÇÃO DE DADOS ---

print("\n Transformando tipos de dados e criando novas colunas ")
# 7. Converta os campos Likes, Comments e Views para 'int' no df_video
df_video = df_video.withColumn("Likes", col("Likes").cast("integer")) \
                   .withColumn("Comments", col("Comments").cast("integer")) \
                   .withColumn("Views", col("Views").cast("integer"))
print("Colunas Likes, Comments e Views de df_video convertidas para inteiro.")

# 8. Converta e renomeie campos no df_comentario
df_comentario = df_comentario.withColumn("Likes", col("Likes").cast("integer")) \
                             .withColumn("Sentiment", col("Sentiment").cast("integer")) \
                             .withColumnRenamed("Likes", "Likes_Comment")
print("Colunas Likes e Sentiment de df_comentario convertidas e 'Likes' renomeada.")

# 9. Crie o campo 'Interaction' no df_video
df_video = df_video.withColumn('Interaction', col('Likes') + col('Comments') + col('Views'))
print("Coluna 'Interaction' criada em df_video.")

# 10. Converta o campo 'Published At' para 'date'
df_video = df_video.withColumn("Published At", col("Published At").cast("date"))
print("Coluna 'Published At' convertida para data.")

# 11. Crie o campo 'Year' extraindo o ano
df_video = df_video.withColumn("Year", year(col("Published At")))
print("Coluna 'Year' criada em df_video.")

# --- ETAPA 3: INTEGRAÇÃO DE DADOS (JOINS) ---

print("\n Integrando os DataFrames ")
# 12. Mescle os dados df_comentario no dataframe df_video
df_join_video_comments = df_video.join(df_comentario, "Video ID", "inner")
print("Join entre df_video e df_comentario realizado")
print("Visualização do resultado do join:")
df_join_video_comments.select("Video ID", "Title", "Likes_Comment", "Sentiment").show(5, truncate=False)

# 13. Leia o arquivo ‘USvideos.csv'
# Precisamos ter cuidado com caracteres especiais, então usamos a opção de escape
df_us_videos = spark.read.csv('USvideos.csv', header=True, inferSchema=True, escape='"')
# Renomeando a coluna 'title' para 'Title' para o join funcionar
df_us_videos = df_us_videos.withColumnRenamed('title', 'Title')
print("Arquivo 'USvideos.csv' carregado e coluna de título renomeada.")

# 14. Mescle os dados df_us_videos no dataframe df_video
df_join_video_usvideos = df_video.join(df_us_videos, "Title", "inner")
print("Join entre df_video e df_us_videos realizado com sucesso!")
print("Visualização do resultado do join:")
df_join_video_usvideos.select("Title", "channel_title", "category_id", "tags").show(5, truncate=False)

# --- ETAPA 4: VALIDAÇÃO E ESCRITA ---

print("\n Validação final e salvamento dos dados ")
# 15. Verifique a quantidade de campos nulos em df_video
print("Contagem de nulos em cada coluna do df_video final:")
df_video.select([count(when(col(c).isNull(), c)).alias(c) for c in df_video.columns]).show()

# 16. Remova a coluna '_c0' (se existir) e salve df_video
if '_c0' in df_video.columns:
    df_video = df_video.drop('_c0')
df_video.write.mode('overwrite').parquet('videos-tratados-parquet')
print("DataFrame 'df_video' final salvo como 'videos-tratados-parquet'")

# 17. Remova a coluna '_c0' (se existir) e salve df_join_video_comments
if '_c0' in df_join_video_comments.columns:
    df_join_video_comments = df_join_video_comments.drop('_c0')
df_join_video_comments.write.mode('overwrite').parquet('videos-comments-tratados-parquet')
print("DataFrame 'df_join_video_comments' final salvo como 'videos-comments-tratados-parquet'")

print("\n--- Tarefa de tratamento e integração concluída! ---")
spark.stop()
