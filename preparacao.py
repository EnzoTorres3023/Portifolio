# --- ETAPA 0: IMPORTAÇÕES E SESSÃO SPARK ---
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month
from pyspark.ml.feature import StringIndexer, VectorAssembler, StandardScaler, PCA
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Iniciar a Sessão Spark
spark = SparkSession.builder \
    .appName("Preparacao de Dados para ML do YouTube") \
    .getOrCreate()

print(f"Sessão Spark iniciada. Versão: {spark.version}")

# --- ETAPA 1: LEITURA E ENGENHARIA DE ATRIBUTOS ---

# 1. Leia o arquivo ‘videos-tratados.snappy.parquet'
print("\n--- 1. Lendo o DataFrame tratado ---")
df_video = spark.read.parquet('videos-tratados-parquet/')
# Garantir que não há nulos que possam quebrar os modelos
df_video = df_video.dropna()
print("Arquivo 'videos-tratados-parquet' carregado.")


# 2. Adicione a coluna 'Month'
print("\n--- 2. Adicionando a coluna 'Month' ---")
df_video = df_video.withColumn("Month", month(col("Published At")))
print("Coluna 'Month' criada.")


# 3. Adicione a coluna "Keyword Index"
print("\n--- 3. Criando o 'Keyword Index' com StringIndexer ---")
string_indexer = StringIndexer(inputCol="Keyword", outputCol="Keyword_Index")
model_indexer = string_indexer.fit(df_video)
df_video = model_indexer.transform(df_video)
print("Coluna 'Keyword_Index' criada.")


# --- ETAPA 2: PRÉ-PROCESSAMENTO PARA MACHINE LEARNING ---

# Para evitar o erro 'IllegalArgumentException:  Data type string is not supported',
# garantimos que as colunas que vamos usar no vetor são numéricas.
df_video = df_video.withColumn("Year", col("Year").cast("integer"))
df_video = df_video.withColumn("Month", col("Month").cast("integer"))
print("\n--- Tipos das colunas 'Year' e 'Month' alteradas para integer ---")


# 4. Crie um vetor chamado "Features"
print("\n--- 4. Montando o vetor de 'Features' ---")
feature_cols = ["Likes", "Views", "Year", "Month", "Keyword_Index"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="Features")
df_video = assembler.transform(df_video)
print("Coluna 'Features' criada.")


# 5. Adicione a coluna "Features Normal" com dados normalizados
print("\n--- 5. Normalizando as 'Features' com StandardScaler ---")
scaler = StandardScaler(inputCol="Features", outputCol="Features_Normal")
scaler_model = scaler.fit(df_video)
df_video = scaler_model.transform(df_video)
print("Coluna 'Features_Normal' criada.")


# 6. Adicione a coluna "Features PCA" com redução de dimensionalidade
print("\n--- 6. Reduzindo a dimensionalidade com PCA ---")
pca = PCA(k=1, inputCol="Features_Normal", outputCol="Features_PCA")
pca_model = pca.fit(df_video)
df_video = pca_model.transform(df_video)
print("Coluna 'Features_PCA' criada.")


# --- ETAPA 3: TREINAMENTO E AVALIAÇÃO DO MODELO ---

# 7. Separe o dataframe em conjuntos de treinamento e teste (80/20)
print("\n--- 7. Separando dados para treino e teste ---")
train_data, test_data = df_video.randomSplit([0.8, 0.2], seed=42)
print(f"Dados de treino: {train_data.count()} registros | Dados de teste: {test_data.count()} registros.")


# 8. Crie, treine e avalie um modelo de Regressão Linear
print("\n--- 8. Treinando e Avaliando o modelo de Regressão Linear ---")
# Criando o modelo
lr = LinearRegression(featuresCol="Features_Normal", labelCol="Comments")
# Treinando o modelo
model_lr = lr.fit(train_data)
# Fazendo as previsões
predictions = model_lr.transform(test_data)

# Avaliando o desempenho
evaluator_r2 = RegressionEvaluator(labelCol="Comments", predictionCol="prediction", metricName="r2")
r2 = evaluator_r2.evaluate(predictions)
print(f"Coeficiente de Determinação (R²): {r2:.4f}")

evaluator_rmse = RegressionEvaluator(labelCol="Comments", predictionCol="prediction", metricName="rmse")
rmse = evaluator_rmse.evaluate(predictions)
print(f"Raiz do Erro Quadrático Médio (RMSE): {rmse:.2f}")


# --- ETAPA 4: SALVANDO O RESULTADO FINAL ---

# 9. Salve o dataframe df_video preparado
print("\n--- 9. Salvando o DataFrame final preparado ---")
df_video.write.mode('overwrite').parquet('videos-preparados-parquet')
print("Arquivo 'videos-preparados-parquet' salvo com sucesso!")


# --- FINALIZAÇÃO ---
print("\n--- Pipeline de preparação para ML concluído com sucesso! ---")
spark.stop()
