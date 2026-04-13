# 1. Importar as bibliotecas e iniciar a Sessão Spark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Leitura e Escrita de Dados do YouTube") \
    .getOrCreate()

print(f"Sessão Spark iniciada. Versão: {spark.version}")

# 2. Ler os dados do arquivo "videos-stats.csv" (sem inferir o esquema)
print("\n Lendo 'videos-stats.csv' pela primeira vez")
df_videos_raw = spark.read.csv('videos-stats.csv')

# 3. Visualizar os primeiros 8 registros do arquivo
print("\n Visualizando os primeiros 8 registros")
df_videos_raw.show(8, truncate=False)

# 4. Visualizar o esquema do arquivo
print("\n Visualizando o esquema inicial")
df_videos_raw.printSchema()

# 5. Ler novamente o arquivo, agora inferindo o esquema e com cabeçalho
print("\n Lendo o arquivo, agora com 'inferSchema' e 'header'")
df_videos = spark.read.csv(
    'videos-stats.csv',
    inferSchema=True,
    header=True
)

# Corrigindo o nome da primeira coluna que o Spark nomeou automaticamente.
df_videos = df_videos.withColumnRenamed('_c0', 'Index')

# Visualizar o esquema corrigido
print("\n Esquema corrigido após inferir os tipos")
df_videos.printSchema()
df_videos.show(5, truncate=False)

# 6. Salvar o arquivo como 'videos-parquet'
print("\n Salvando o DataFrame em formato Parquet ---")
df_videos.write.mode('overwrite').parquet('videos-parquet')
print("Arquivo 'videos-parquet' salvo")

# 7. Ler e visualizar o arquivo 'videos-parquet'
print("\n Lendo o arquivo Parquet de volta para um DataFrame")
df_videos_parquet = spark.read.parquet('videos-parquet')
df_videos_parquet.show(5, truncate=False)
df_videos_parquet.printSchema()

# 8. Salvar o DataFrame como uma tabela no Spark Catalog
print("\n Salvando o DataFrame como uma tabela")
df_videos.write.mode('overwrite').saveAsTable('tb_videos')
print("Tabela 'tb_videos' criada")

# 9. Listar as tabelas do Spark Catalog para verificar
print("\n Listando as tabelas disponíveis no catálogo")
lista_de_tabelas = spark.catalog.listTables()
for tabela in lista_de_tabelas:
    print(tabela)

# 10. Utilizar o Spark SQL para ler a tabela ‘tb_videos’
print("\n Consultando a tabela 'tb_videos' com Spark SQL")
resultado_sql = spark.sql("SELECT Title, Views, Likes FROM tb_videos WHERE Likes > 1000000 ORDER BY Views DESC")
resultado_sql.show(10, truncate=False)

# 11. Ler o arquivo ‘comments.csv'
print("\n Lendo o arquivo 'comments.csv'")
df_comments = spark.read.csv(
    'comments.csv',
    inferSchema=True,
    header=True
)
# Corrigindo potenciais nomes de colunas problemáticos em comments.csv
if '_c0' in df_comments.columns:
    df_comments = df_comments.withColumnRenamed('_c0', 'Comment_Index')

df_comments.printSchema()
df_comments.show(5, truncate=False)

# 12. Salvar o arquivo como ‘comments-parquet'
print("\n Salvando os comentários em formato Parquet")
df_comments.write.mode('overwrite').parquet('comments-parquet')
print("Arquivo 'comments-parquet' salvo")

print("\n Tarefa Concluída!")
spark.stop()