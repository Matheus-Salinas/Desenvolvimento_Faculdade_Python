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

# Exibir tipo dos valores das colunas
df.printSchema()

def tabela_geral(df):
    # Renomear as colunas
    df = df.withColumnRenamed('Team', 'Selecao')\
        .withColumnRenamed('#', 'Numero_da_Camisa')\
        .withColumnRenamed('FIFA Popular Name', 'Nome_FIFA')\
        .withColumnRenamed('Birth Date', 'Nascimento')\
        .withColumnRenamed('Shirt Name', 'Nome_Camiseta')\
        .withColumnRenamed('Club', 'Time')\
        .withColumnRenamed('Height', 'Altura')\
        .withColumnRenamed('Weight', 'Peso')\
        .withColumnRenamed('Pos.', 'Posicao')\
        .withColumn('Ano', substring('Nascimento', -4, 4))\
        .withColumn('Nascimento', to_date(col('Nascimento'), "dd.MM.yyyy"))\
        .withColumn('Mes_de_nascimento', date_format(col('Nascimento'), 'MMM'))\
        .withColumn('Dia_de_nascimento', day(col('Nascimento')))\
        .withColumn('Selecao - Peso - Altura', concat_ws(' - ', 'Selecao', 'Peso', 'Altura'))\
        .withColumn('Idade', floor(datediff(current_date(), col('Nascimento')) / 365))  # Calcula a idade

    # Alterar Tipo dos valores das colunas
    df = df.withColumn('Ano', col('Ano').cast(IntegerType()))
    df.printSchema()

    # Window 1 - Número da Linha (Ranking por Altura)
    num_linha = Window.partitionBy('Selecao').orderBy(desc('Altura'))
    df = df.withColumn('Rankin_Altura', row_number().over(num_linha))

    # Window 2 - Ranking por Peso
    rank_01 = Window.partitionBy('Selecao').orderBy(desc('Peso'))
    df = df.withColumn('Rank_Peso', rank().over(rank_01))

    # Window 3 - Ranking por Peso (Dense Rank)
    rank_02 = Window.partitionBy('Selecao').orderBy(desc('Idade'))
    df = df.withColumn('Rank_Idade', dense_rank().over(rank_02))

    df.show(50)
    print("Tabela Geral")
    return df

def gerar_tabela_grupo_peso(df):
    # Agrupado por Peso
    df_peso = df.groupBy('Selecao') \
        .agg(
            round(avg('Peso'), 0).alias('Media_Peso'),  # Calcula a média e renomeia a coluna
             max('Peso').alias('Maximo_Peso'),
             min('Peso').alias('Minimo_Peso'))
    df_peso.show()
    return df_peso

def gerar_tabela_group_altura(df):
    # Agrupado por Altura
    df_altura = df.groupBy('Selecao') \
        .agg(
            round(avg('Altura'), 0).alias('Media_Altura'),  # Calcula a média e renomeia a coluna
             max('Altura').alias('Maximo_Altura'),
             min('Altura').alias('Minimo_Altura'))
    df_altura.show()
    return df_altura

# Executar as funções
df = tabela_geral(df)
print('Tabela Geral Gerada')
df_peso = gerar_tabela_grupo_peso(df)
print('Tabela Group By Peso Gerada')
df_altura = gerar_tabela_group_altura(df)
print('Tabela Group By Altura Gerada')

# Filtros adicionais (opcional)
df.filter((col('Selecao') == "Brazil") & (col('Altura') > 180)).show()
df.filter((col('Selecao') == "Brazil") | (col('Selecao') == "Argentina")).show(5)
df.filter((col('Selecao') == "Brazil") & (col('Posicao') == 'DF') | (col('Selecao') == 'Argentina') & (col('Posicao') == 'DF')).show()

# Fechar a SparkSession
spark.stop()