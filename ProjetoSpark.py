import os
from pyspark.sql import SparkSession


variables = [
    'JAVA_HOME',
    'HADOOP_HOME',
    'SPARK_HOME',
    'SPARK_DIST_CLASSPATH',
    'PYSPARK_PYTHON',
    'PYSPARK_DRIVER_PYTHON'
]

print('Variáveis do Ambiente')

for v in variables:
    value = os.environ.get(v,'Não definida')
    print(f'{v} = {value}')


spark = SparkSession.builder.getOrCreate()
print(f'Versão do Spark: {spark.version}')