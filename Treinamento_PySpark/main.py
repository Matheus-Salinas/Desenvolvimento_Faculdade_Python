from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

# Definir o caminho correto do Java
os.environ["JAVA_HOME"] = r"C:\Program Files\Zulu\zulu-21"  # Ajuste para a pasta correta do JDK

# Criar a SparkSession
spark = (
    SparkSession.builder
    .master('local')
    .appName('Spark_01')
    .getOrCreate()
)

# Ler o CSV (verifique se o caminho está correto)
df = spark.read.csv('data/Selecao_Fifa.csv', header=True, inferSchema=True)

# Exibir tipo do valores das colunas
df.printSchema()

# Renomear as colunas
df = df.withColumnRenamed('Team', 'Selecao')\
    .withColumnRenamed('#', 'Número_da_Camisa')\
    .withColumnRenamed('FIFA Popular Name', 'Nome_FIFA')\
    .withColumnRenamed('Birth Date', 'Nascimento')\
    .withColumnRenamed('Shirt Name', 'Nome_Camiseta')\
    .withColumnRenamed('Club', 'Time')\
    .withColumnRenamed('Height', 'Altura')\
    .withColumnRenamed('Weight', 'Peso')\
    .withColumnRenamed('Pos.', 'Posicao')\
    .withColumn('Ano', substring('Nascimento', -4,4))\
    .withColumn('Nascimento', to_date(col('Nascimento'), "dd.MM.yyyy"))\
    .withColumn('Mês de nascimento', month(col('Nascimento')))\
    .withColumn('Dia de nascimento', day(col('Nascimento')))
df.show(50)
print("Base renomeada")
# Filtro usando 'E'
df.filter((col('Selecao') == "Brazil") & (col('Altura') > 180)).show()
print("Filtros por Selecao Brazileira com altura")
#Filtro usando 'Ou'
df.filter((col('Selecao') == "Brazil")|
          (col('Selecao') == "Argentina")).show(5)
print("Filtro Usando OU com |")
#Filtro usando 'E' + 'Ou'
df.filter((col('Selecao') == "Brazil") & (col('Posicao') == 'DF') | (col('Selecao') == 'Argentina') & (col('Posicao') == 'DF')).show()
print("Filtro combinando 'ou' com 'e'")
#Criação de novas colunas
df.withColumn('Coluna Nova', lit(col('Altura') - col('Peso'))).show(5)
print("Nova coluna criada")
df.withColumn('Sub', substring('Selecao', 1, 3)).show(5)
# Criar colunas concatenadas com espaco
df.withColumn('Separadores', concat_ws(' - ','Selecao','Peso','Altura')).show(5)
#Criar colunas concatenadas sem espaco
df.withColumn('Valores Concatenados(Peso,Selecao,Altura)', concat('Peso','Selecao','Altura')).show(5)
# Alterar Tipo dos valores das colunas
df = df.withColumn('Ano', col('Ano').cast(IntegerType()))
df.printSchema()
#Alterar a data de nascimento para 'YYYY-MM-DD'
df = df.withColumn('Nascimento', to_date(col('Nascimento'), "dd.MM.yyyy"))
df.printSchema()
df.show()

df = df.withColumn('Mês de nascimento', month(col('Nascimento')))\
    .withColumn('Dia de nascimento', day(col('Nascimento')))
df.show()
#Drop de colunas 
df = df.drop('Nascimento')
df.show()

num_linha = Window.partitionBy('Selecao').orderBy(desc('Altura'))

# Adiciona uma nova coluna 'Numero_Linha' com o número da linha dentro de cada partição
df.withColumn('Numero_Linha', row_number().over(num_linha)).show(50)

#Window 2_Ranking
rank_01 = Window.partitionBy('Selecao').orderBy(desc('Peso'))
df.withColumn('rank', rank().over(rank_01)).show(50)
print("Rankin por Peso")
#Segundo modelo de rankin
rank_02 = Window.partitionBy('Selecao').orderBy(desc('Peso'))
df.withColumn('Rank2', dense_rank().over(rank_02)).show(50)
print('Segundo lugar contabiliza ',)
#Agrupado por Peso
df.groupBy('Selecao') \
               .agg(avg('Peso').alias('Média Peso'),  # Calcula a média e renomeia a coluna
                    max('Peso').alias('Maximo Peso'),
                    min('Peso').alias('Minimo Peso')).show()
#Agrupado por Altura
df.groupBy('Selecao') \
               .agg(avg('Altura').alias('Média Altura'),  # Calcula a média e renomeia a coluna
                    max('Altura').alias('Maximo Altura'),
                    min('Altura').alias('Minimo Altura')).show()
df.select('*').where(col('Altura') <= 190).show()
