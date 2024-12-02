import os, re, sys, time
from decimal import Decimal
from datetime import date
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, DecimalType
from sqlalchemy import create_engine#, Column, Integer, Sequence, String, Date, Float, BIGINT, Numeric
from sqlalchemy.sql.expression import text

# Para contornar a falta de connective do SPARK com TRINO / NESSIE + MINIO
# Creiei funções auxiliares para simular a ingestão no S3 ICEBERG TRINO
def list_to_sqlvalues(x_list:list):
     x_result = []
     for x_item in x_list:
        xxx_item = ''
        for xx_item in x_item:
            if type(xx_item) is Decimal:
                xx_item = "cast("+str(xx_item)+" as decimal(10,2))"
            elif type(xx_item) is int: 
                True
            elif re.match(r'^\'\d{4}\-\d{2}\-\d{2}\'$', str(xx_item)):
                xx_item = "date "+str(xx_item)
            elif type(xx_item) is date or re.match(r'^\d{4}\-\d{2}\-\d{2}$', str(xx_item)):
                xx_item = "date '"+str(xx_item)+"'"
            elif not type(xx_item) is int: 
                xx_item = "'"+str(xx_item)+"'"
            #if re.match(r'\'\d{4}\-\d{2}', str(xx_item)): xx_item = "date "+str(xx_item)
            xxx_item += ","+str(xx_item)
        x_result.append(xxx_item)
     return str('('+'),('.join(x_result)+')').replace('(,','(')

def BulkInsertTRINO(x_dict:dict, x_tablename:str, x_cols:dict,x_batch=10):
    x_count = 0
    x_cols = ','.join(x_cols)
    x_query_insert = ''
    for i in range(1,len(x_dict)//x_batch+2):
        xx_dict = x_dict[(i-1)*x_batch:i*x_batch]
        xxx_dict = list_to_sqlvalues(xx_dict)
        x_count += len(xx_dict)
        print(i, len(xx_dict))
        if len(xx_dict)==0:break
        try:
            x_query_insert = f"""INSERT INTO {x_tablename} ({x_cols}) VALUES {xxx_dict}"""
            trino_conn.execute(text(x_query_insert))
        except:
            print('! Falha no INSERT:', x_query_insert)
            return 
    print('* Sucesso no Insert, total de registros:',x_count)
    return 'OK'

# SET Variables
print("* SET Variables")
TB_VENDAS = "tb001_vendas"
# Set Directory of Project
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
# JDBC SQLite from: https://github.com/xerial/sqlite-jdbc/releases
SQLITE_JDBC = f"{SRC_DIR}/jdbc-files/sqlite-jdbc-3.47.1.0.jar"
# JDBC TRINO from: https://trino.io/docs/current/client/jdbc.html
#TRINO_JDBC = f"{SRC_DIR}/jdbc-files/jdbc/trino-jdbc-466.jar"

# SET SparkSession
print("* SET SparkSession")
spark = SparkSession.builder\
    .config("spark.log.level", "ERROR")\
    .config("spark.jars", f"{SQLITE_JDBC}")\
    .appName("SQLITE_SPARK")\
    .getOrCreate()
    #.config('spark.jars.packages', 'org.xerial:sqlite-jdbc:3.47.1.0')\ #-- Outro modo de usar JDBC DO SQLITE, porém o arquivo é baixado em toda execução

# LOAD DataFrame, Camada Bronze, referencia de dados originais
print(f"* LOAD DataFrame: SQLITE '{TB_VENDAS}'")
df_bronze = spark.read\
    .format('jdbc')\
    .option("customSchema", "id INTEGER, data_venda STRING, id_produto INTEGER, id_cliente INTEGER, quantidade INTEGER, valor_unitario DECIMAL(10,2), valor_total DECIMAL(10,2), id_vendedor INTEGER, regiao VARCHAR(50)")\
    .options(driver="org.sqlite.JDBC", dbtable=TB_VENDAS, url=f"jdbc:sqlite:{SRC_DIR}/sqlite-files/vendas.db")\
    .load()

# SHOW SCHEMA OF TABLE
print(f"SCHEMA DA TABELA '{TB_VENDAS}':")
df_bronze.printSchema()

# Count Rows DF_BRONZE for report
df_bronze_rows = df_bronze.count()

## 1. Converta as datas para o formato ISO.
# Transform STRING DATE "data_venda" TO DATE ISO 8601 FORMAT YYYY-MM-DD
print('* Convertendo coluna "data_venda para DATE ISO8601')
df_prata = df_bronze.withColumn("data_venda", F.to_date("data_venda", "yyyy-MM-dd"))

## 3. Remova quaisquer dados duplicados.
# Remove Duplicates, if all rows contains same value by ID
print('* Removendo duplicados')
df_prata = df_prata.dropDuplicates(['id'])

print("")
print('TOP 5 LINHAS DA TABELA (AMOSTRA)')
df_prata.show(5)

# Count Rows DF_PRATA for report
df_prata_rows = df_prata.count()

## 2. Calcule o total de vendas por dia.
# Relatório do Total de Vendas por data de venda
print("TOTAL DE VENDAS, POR DATA_VENDA:")
(df_prata
    .groupBy("data_venda")
    .agg(F.count("*").alias("# QTD"), F.sum("valor_total").alias("R$ TOTAL"))
    .orderBy("data_venda")
    .show())

# Resumo de total de linhas ingeridas
print(f"""RESUMO\nQtd linhas na camada Bronze: {df_bronze_rows}\nQtd linha na camada Prata: {df_prata_rows}""")
print("- - - - - - - -- - - - - - - - - - - - - - - -")

## Para simular o S3 ICEBERG localmente, configurei o ambiente NESSIE + MINIO + TRINO
## Entretanto não foi possivel conectar Spark ao TRINO / NESSIE + MINIO, com as versões das bibliotecas atuais
## Então crei funções para simular essa ingestão
##
#  Caso contrário faria uso da estragia abaixo:
#  Criar tempView
#  df.createOrReplaceTempView("tempview");
#  Fazer a ingestão diretamente da tempView
#  spark.sql("CREATE or REPLACE TABLE local.db.one USING iceberg AS SELECT * FROM tempview");
##
##...continuando com a solução alternativa...

# Conetando ao Trino 
print("* Conectando ao TRINO, nosso processador de querys de arquivos Iceberg")
trino_engi = create_engine('trino://admin@localhost:8080/iceberg')
try:   
    trino_conn = trino_engi.connect()
    trino_conn.execute(text("""SELECT 1""")).fetchall()
except:
    print("! Falha ao connectar no TRINO. Atenção!!! Verifique se o serviço esta ativo !")
    time.sleep.time(10)
    sys.exit()

print("")
print("* Preparando ingestão no S3 ICEBERG")

# Create New SCHEMA
print("* Criando Novo SCHEMA 'db'")
query_trino_create_schema_db = "CREATE SCHEMA IF NOT EXISTS iceberg.db WITH (location = 's3://warehouse/data')"
try:
    trino_conn.execute(text(query_trino_create_schema_db))
except:
    print("! Falha ao criar Novo SCHEMA 'db' no TRINO")

# Create Table ICEBERG
print("* Criando Tabela ICEBERG 'tb_vendas', simulando o S3 ICEBERG localmente")
query_trino_create_table_vendas = """
CREATE TABLE IF NOT EXISTS iceberg.db.tb_vendas (
	dt_processamento DATE,
	year_dt_venda INTEGER NOT NULL,
	month_dt_venda INTEGER NOT NULL,
	day_dt_venda INTEGER NOT NULL,
	id INTEGER NOT NULL, 
	data_venda DATE NOT NULL, 
	id_produto INTEGER NOT NULL, 
	id_cliente INTEGER NOT NULL, 
	quantidade INTEGER NOT NULL, 
	valor_unitario DECIMAL(10,2), 
	valor_total DECIMAL(10,2), 
	id_vendedor INTEGER NOT NULL, 
	regiao VARCHAR(50)
)
WITH (
	format = 'PARQUET',
 	partitioning=ARRAY['year_dt_venda','month_dt_venda','day_dt_venda'],
	sorted_by = ARRAY['year_dt_venda','month_dt_venda','day_dt_venda']
)
"""
try:
    trino_conn.execute(text(query_trino_create_table_vendas))
except:
    print("! Falha ao Criar Tabela de VENDAS no TRINO")


## Manipulando a tabela para ingestão no S3 ICEBERG TRINO
# Este é mais um ajuste para adaptar a uma solução local
# Uma vez que no S3 AWS é capaz reconhecer em única coluna de data os 3 níveis (ano, mês e dia) 
# sem maiores esforços para particionar
print("* Adicionando colunas de ANO, MÊS e DIA para PARTICIONAMENTO de ANO, MÊS, DIA")
print("* Adicionando coluna de data de processamento 'dt_processamento'")
df_prata2 = df_prata.withColumns({'dt_processamento':F.current_date(), 'year_dt_venda':F.year(df_prata.data_venda).cast(IntegerType()), 'month_dt_venda':F.month(df_prata.data_venda).cast(IntegerType()), 'day_dt_venda':F.day(df_prata.data_venda).cast(IntegerType())})

print("")
print('TOP 5 REGISTROS: (última amostra antes de ingestão)')
df_prata2.show(5)

# Setando variaveis para ingestão
df_prata_cols = df_prata2.columns

# Convertendo DataFrame para tuplas
df_prata_rdd = df_prata2.rdd
df_prata_tupla = df_prata_rdd.map(tuple).collect()

BulkInsertTRINO(df_prata_tupla, 'iceberg.db.tb_vendas', df_prata_cols)

## TRINO também possui muitas restrições em relação ao sqlalchemy
## "autoload_with" ainda não funciona para o TRINO
## Então fui para a altertiva de hardcode... 
## Para concluir a tarefa de ingestão ainda em ARQUIVO S3 ICEBERG LOCALMENTE
"""
class Vendas(Base):
    __tablename__ = 'tb_vendas'
    dt_processamento = Column(Date)
    year_dt_venda = Column(Date)
    month_dt_venda = Column(Date, nullable=False)
    day_dt_venda = Column(Date, nullable=False)
    id = Column(Integer, nullable=False, primary_key=True)
    data_venda = Column(Date, nullable=False)
    id_produto = Column(Integer, nullable=False)
    id_cliente = Column(Integer, nullable=False) 
    quantidade = Column(Integer, nullable=False) 
    valor_unitario = Column(Numeric(10,2))
    valor_total = Column(Numeric(10,2))
    id_vendedor = Column(Integer, nullable=False)
    regiao = Column(String(50))
"""

print("QUERY ATHENA: (script também funciona no AWS ATHENA)")
query_athena = """
WITH cte AS (
    SELECT 
        year_dt_venda
        ,month_dt_venda
        ,valor_total, quantidade
    FROM iceberg.db.tb_vendas
)
SELECT
    CONCAT(CAST(year_dt_venda as varchar),'-',CAST(month_dt_venda as varchar)) AS ano_mes
    ,SUM(valor_total) AS total_vl
    ,SUM(quantidade) AS total_qtd
    ,SUM(valor_total)/SUM(quantidade) AS tkt_medio
FROM cte
GROUP BY year_dt_venda, month_dt_venda
ORDER BY year_dt_venda desc, month_dt_venda desc
"""
print(query_athena)
print("* Executando query...")
result_athena = trino_conn.execute(text(query_athena)).fetchall()
columns = ['ANO_MES','TOTAL_VL','TOTAL_QTD','TKT_MEDIO']
result = [{columns[index]: column for index, column in enumerate(value)} for value in result_athena]
df = spark.createDataFrame(result)
df = df.withColumns({'TOTAL_VL':df["TOTAL_VL"].cast(DecimalType(10,2)), 'TKT_MEDIO':df["TKT_MEDIO"].cast(DecimalType(10,2))})
print("")
print('RESULTADO QUERY ATHENA:')
df.select(df["ANO_MES"],df["TOTAL_VL"],df["TOTAL_QTD"],df["TKT_MEDIO"]).show()

