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
    europa = ['Sweden', 'Germany', 'France', 'Belgium', 'Croatia', 'Spain', 'Denmark', 'Iceland', 'Switzerland', 'England', 'Poland', 'Portugal', 'Serbia']
    asia = ['Russia', 'IR Iran', 'Nigeria', 'Korea Republic', 'Saudi Arabia', 'Japan', ]
    africa = ['Senegal', 'Morocco', 'Tunisia', 'Egypt']
    oceania = ['Australia']
    america_norte = ['Panama', 'Mexico', 'Costa Rica']
    america_sul = ['Argentina', 'Peru', 'Uruguay', 'Brazil', 'Colombia']
    df = df.withColumn('Continente', when(col('Selecao').isin(europa), 'Europa')\
             .when(col('Selecao').isin(asia), 'Ásia')\
             .when(col('Selecao').isin(africa), 'África')\
             .when(col('Selecao').isin(oceania), 'Oceania')\
             .when(col('Selecao').isin(america_norte), 'América do Norte')\
             .when(col('Selecao').isin(america_sul), 'América do Sul')\
             .otherwise('Verificar'))
    df.show()
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

def tabelas_usando_union(df):
    df_america_sul = df.filter('Continente = "América do Sul"')
    df_america_sul.select('Selecao').distinct().show()
    df_america_norte = df.filter('Continente = "América do Norte"')
    df_america_norte.select('Selecao').distinct().show()

    #Base com union
    df_americas = df_america_sul.union(df_america_norte)
    df_americas.select('Selecao').distinct().show()
    return df_americas

def criar_tabela_filtrada(df, selecao, numeros_excluidos=None):
    tabela_filtrada = df.filter(col('Selecao') == selecao) \
        .drop('Nascimento', 'Time', 'Mes_de_nascimento', 'Dia_de_nascimento', 
              'Selecao - Peso - Altura', 'Idade', 'Rankin_Altura', 'Rank_Peso', 'Rank_Idade') \
        .withColumnRenamed('Numero_da_Camisa', 'Numero')
    
    if numeros_excluidos:
        tabela_filtrada = tabela_filtrada.filter(~col('Numero').isin(numeros_excluidos))
    
    return tabela_filtrada

def tabela_usando_joins(df):
    # Criando as bases usadas para os joins
    tabela_argentina = criar_tabela_filtrada(df, "Argentina")
    tabela_brasil = criar_tabela_filtrada(df, "Brazil", numeros_excluidos=[22, 5, 7])
    
    # Exibindo as tabelas
    print("Tabela Argentina:")
    tabela_argentina.show(40)
    print("Tabela Brasil:")
    tabela_brasil.show(40)
    
    # Realizando os joins
    df_join_simples = tabela_argentina.join(tabela_brasil, "Numero")
    df_inner_join = tabela_argentina.join(tabela_brasil, "Numero", "inner")
    df_left_join = tabela_argentina.join(tabela_brasil, "Numero", "left")
    df_right_join = tabela_argentina.join(tabela_brasil, "Numero", "right")
    df_full_join = tabela_argentina.join(tabela_brasil, "Numero", "full")
    df_anti_join = tabela_argentina.join(tabela_brasil, "Numero", "anti")
    
    # Exibindo os resultados dos joins
    print("Join Simples:")
    df_join_simples.show(40)
    print("Inner Join:")
    df_inner_join.show(40)
    print("Left Join:")
    df_left_join.show(40)
    print("Right Join:")
    df_right_join.show(40)
    print("Full Join:")
    df_full_join.show(40)
    print("Anti Join:")
    df_anti_join.show(40)

# Executar as funções
df = tabela_geral(df)
print('Tabela Geral Gerada')
df_peso = gerar_tabela_grupo_peso(df)
print('Tabela Group By Peso Gerada')
df_altura = gerar_tabela_group_altura(df)
print('Tabela Group By Altura Gerada')

# Fechar a SparkSession
spark.stop()