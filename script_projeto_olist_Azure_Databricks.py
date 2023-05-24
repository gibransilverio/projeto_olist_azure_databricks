# Montando Data Lake no Databricks:

# 0 - Registrar o aplicativo no azure active directory e pegar o 'client_id', após isto colocar no 'fs.azure.account.oauth2.client.id' (Ex: olist_app)


# 1 - Criar scope e secret(key), após isto colocar no  'fs.azure.account.oauth2.client.secret': dbutils.secrets.get(scope="name_scope",key="name_secret")'
#     https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes (Documentação - Url de criar scope + key Databricks)
#     Ulr -> Localizada dentro do Azure Databricks  ----> https://adb-6935834773204596.16.azuredatabricks.net#secrets/createScope (olist_scope) (Manage Principal - All Users) ---> Resource ID + VAULT URL da Key Vault (Criar Key Vault)
#     Criar Secret Key -> (olist-secret) na Key Vault ----> Depois na Key Vault criada ir em criar secret ----> Value: O valor está em  Certificates & secrets no AD - App Registrado 
#     Ir no AD - App Registrado (olist_app) ir em Certificates & secrets para criar um novo secret (olist-app-secret) para colocar no (olist-secret)

# 2 - Ir na Key Vault - Overview - Directory ID ----> Substituir  a URL padrão e colcoar o Directory ID "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/directory_id_key_vault/oauth2/token"


# 3 - Pegar URL da Storage Account - Endpoints - URL - Data Lake Storage ----> Substituir em ----> source = "abfss://landing@oliststorageaccount2.dfs.core.windows.net/", 
# 						                                                   source = "abfss://container_name@url_storage_account.dfs.core.windows.net/",


# 4 - Ponto de montagem: mount_point = "/mnt/landing", # Nome do Container dentro do Data Lake Storage da Azure                                         
#  		                 extra_configs = configs)      # Nome dado ao escopo de configuração citado acima


# 5 - Liberar permissões dos containers em Storage Account -  Manage ACL - Other - Access Permissions - Default Permissions (Chield Directory) 
# 

# COMMAND ----------

# Configurações para montar os containers do Data Lake Storage no Databricks
'''
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "60c2523e-a4b9-488b-8949-c57705ae2c18",  
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="olist_scope",key="olist-secret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/2da3dd61-f5b3-42a5-8b14-880fa1054f9a/oauth2/token"} 





'''


# COMMAND ----------

# Montando Landing
'''
dbutils.fs.mount(
  source = "abfss://landing@oliststorageaccount.dfs.core.windows.net/", 
  mount_point = "/mnt/landing",                                         
  extra_configs = configs)                                              
'''

# COMMAND ----------

# Verificando o Landing

'''
dbutils.fs.ls("/mnt/landing/")
'''

# COMMAND ----------

# Montando o Processing
'''
dbutils.fs.mount(
  source = "abfss://processing@oliststorageaccount.dfs.core.windows.net/", 
  mount_point = "/mnt/processing",                                         
  extra_configs = configs)  
'''

# COMMAND ----------

# Verificando o Processing
'''
dbutils.fs.ls("/mnt/processing/")
'''

# COMMAND ----------

# Montando o Curated 
'''
dbutils.fs.mount(
  source = "abfss://curated@oliststorageaccount.dfs.core.windows.net/", 
  mount_point = "/mnt/curated",                                         
  extra_configs = configs)  
  '''

# COMMAND ----------

# Verificando o Processing
'''
dbutils.fs.ls("/mnt/curated/")
'''

# COMMAND ----------

# DBTITLE 1,Desmotando Datalake
'''
dbutils.fs.unmount("/mnt/name_container")

'''


# COMMAND ----------

# DBTITLE 1,Lendo CSV do container Landing 
# Criando os dataframes atráves dos arquivos csv vindo das tabelas do Azure SQL Database 
# Tais tabelas foram enviadas para o container através do Azure SQL Database onde foi criado um pipeline no ADF - Azure Data Factory que exportou as tabelas pro container 'landing' em formato '.csv'

df_customers = spark.read.format('csv').option('inferSchema', 'True').option('header', 'true').option('delimiter',',').load('/mnt/landing/dbo.olist_customers_dataset.csv')
df_geolocation = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_geolocation_dataset.csv")
df_order_items = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_order_items_dataset.csv")
df_order_payments = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_order_payments_dataset.csv")
df_order_reviews = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_order_reviews_dataset.csv")
df_orders = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_orders_dataset.csv")
df_sellers = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.olist_sellers_dataset.csv")
df_product_category_name_translation = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/landing/dbo.product_category_name_translation.csv")



# COMMAND ----------

df_sellers.display()

# COMMAND ----------

# Mostrando o Schema (Estrutura de Dados)

df_customers.printSchema()

# COMMAND ----------

# DBTITLE 1,Convertendo DF para tabelas no Data LakeHouse - Create SQL Temp Views
# Criando uma view temporária do arquvio customers 

df_customers.createOrReplaceTempView('customers_tempview')

# COMMAND ----------

# Ver a Temp View de outras maneiras com o spark:

# spark.sql('SELECT * FROM customers_tempview').show(df_customers.count(), False) # Mostrar todas as linhas padrão do .show() é 20
# spark.sql('SELECT * FROM customers_tempview').display()

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM customers_tempview;

# COMMAND ----------

# DBTITLE 1,Criando Database pelo Databricks
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS customers_db;

# COMMAND ----------

# DBTITLE 1,Criando as tabelas usando arquivo 'CSV' - Container Landing
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.customers 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_customers_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true");

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.geolocation 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_geolocation_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_items 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_order_items_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_payments 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_order_payments_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_reviews 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_order_reviews_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.orders 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_orders_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.sellers 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.olist_sellers_dataset.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.product_category_name_translation 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/landing/dbo.product_category_name_translation.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# DBTITLE 1,Dropando as tabelas 
# Exemplo para deletar as tabelas da base de dados 'customers_db'

'''
%sql
DROP TABLE IF EXISTS customers_db.geolocation     
'''


# COMMAND ----------

# DBTITLE 1,Verificando os dados:
# MAGIC %sql
# MAGIC SELECT COUNT(customer_id) FROM customers_db.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM customers_db.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE customers_db.customers;

# COMMAND ----------

# DBTITLE 1,Criando um dataframe através da tabela da base 'customers_db':
df_customers_sql = spark.table('customers_db.customers')

df_customers_sql.display()

# COMMAND ----------

# DBTITLE 1,Filtrando o Dataframe 'df_customers_sql' 
# MODO SPARK SQL - spark.sql('select').show():

''' spark.sql('select distinct(*) from customers_db.customers').show() '''

# MODO PYSKAR SQL - df.select('colunas'):
''' Pyspark + select * ---> df_customers_sql.select(df_customers_sql['*']).distinct().show() '''
'''                         df_customers_sql.select(df_customers_sql['*']).display() '''
'''                         df_customers_sql.select(df_customers_sql['*']).distinct().display() '''
'''                         df_customers_sql.select(df_customers_sql['customer_state']).distinct().show() # distinct() traz os dados únicos sem duplicação  '''
'''                         df_customers_sql.select('customer_id','customer_state').distinct().show()'''


# PySpark usa ----> .select('')
# Coluna específica:

df_customers_sql.select('customer_state').distinct().show() # distinct() traz os dados únicos sem duplicação 



# COMMAND ----------

# df_customers_sql.select(col('customer_state') == 'RJ').display()  ----> Por isso o uso do filter pois assim só nos traz True e False 

'''
from pyspark.sql.functions import col

df_customers_sql.select(col('customer_state') == 'RJ').display() 
'''

# COMMAND ----------

# Fução filter() + col() 
# col() para selecionar coluna e fazer filtragem, usando operadores relacionais como por exemplo

from pyspark.sql.functions import col

df_customers_sql2 = df_customers_sql.filter(col("customer_state") == "RJ") # Colocando a filtragem em um novo dataframe 


df_customers_sql2.display() # Or display(df_customers_sql2)

# COMMAND ----------

# DBTITLE 1,Converter Dataframe para arquivo parquet para processamente em Data Lake - Container - Processing
# df_customers = spark.read.format('csv').option('inferSchema', 'True').option('header', 'true').option('delimiter',',').load('/mnt/landing/dbo.olist_customers_dataset.csv')
# write.mode().parquet()
# Full-parquet

df_customers.write.mode("overwrite").parquet("/mnt/processing/customers.parquet")

# COMMAND ----------

df_geolocation.write.mode("overwrite").parquet("/mnt/processing/geolocation.parquet")
df_order_items.write.mode("overwrite").parquet("/mnt/processing/order_items.parquet")
df_order_payments.write.mode("overwrite").parquet("/mnt/processing/order_payments.parquet")
df_order_reviews.write.mode("overwrite").parquet("/mnt/processing/order_reviews.parquet")
df_orders.write.mode("overwrite").parquet("/mnt/processing/orders.parquet")
df_sellers.write.mode("overwrite").parquet("/mnt/processing/sellers.parquet")
df_product_category_name_translation.write.mode("overwrite").parquet("/mnt/processing/product_category_name_translation.parquet")



# COMMAND ----------

# DBTITLE 1,Usando um Dataframe Filtrado  'df_customers_sql2' e escrevendo ele em parquet - Container - Processing
# Filtered-parquet

df_customers_sql2.write.mode('overwrite').parquet('/mnt/processing/customers_RJ.parquet')

# COMMAND ----------

# DBTITLE 1,Convertendo um arquivo parquet '/mnt/processing/customers_RJ.parquet' em um Dataframe
df_customers_parq = spark.read.parquet('/mnt/processing/customers_RJ.parquet')

df_customers_parq.display()

# COMMAND ----------

# DBTITLE 1,Criando as tabelas usando arquivo 'PARQUET' - Container Processing
# MAGIC %sql
# MAGIC -- Full parquet -- Sem filtragem
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.customers_pqt USING PARQUET OPTIONS (path "/mnt/processing/customers.parquet", header "true", inferSchema "true")
# MAGIC 
# MAGIC -- OUTRO MODO DE CRIAÇÃO: 
# MAGIC -- %sql
# MAGIC -- CREATE TABLE IF NOT EXISTS customers_db.customers_pqt
# MAGIC -- USING PARQUET
# MAGIC -- LOCATION '/mnt/processing/customers.parquet'
# MAGIC -- OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.geolocation_pqt USING PARQUET OPTIONS (path "/mnt/processing/geolocation.parquet", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_items_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_items.parquet", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_payments_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_payments.parquet", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.order_reviews_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_reviews.parquet", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC 
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.orders_pqt USING PARQUET OPTIONS (path "/mnt/processing/orders.parquet", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.sellers_pqt USING PARQUET OPTIONS (path "/mnt/processing/sellers.parquet", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC -- Full parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.product_category_name_translation_pqt USING PARQUET OPTIONS (path "/mnt/processing/product_category_name_translation.parquet", header "true", inferSchema "true")

# COMMAND ----------

# DBTITLE 1,Criando a tabela usando arquivo 'PARQUET' - Container Processing - Filtrado - 'customers_RJ..parquet'
# MAGIC %sql
# MAGIC -- Filtered parquet
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.customers_RJ_pqt USING PARQUET OPTIONS (path "/mnt/processing/customers_RJ.parquet", header "true", inferSchema "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE customers_db.customers_pqt

# COMMAND ----------

# DBTITLE 1,Verificando as tabelas criadas:
# MAGIC %sql
# MAGIC SELECT * FROM customers_db.customers_pqt

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE customers_db.customers_RJ_pqt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_db.customers_RJ_pqt

# COMMAND ----------

# DBTITLE 1,Comando Refresh em tabelas - Caso que não mostra o 'select'
# MAGIC %sql
# MAGIC REFRESH TABLE customers_db.customers_RJ_pqt

# COMMAND ----------

# DBTITLE 1,Exemplo de flexibilidade entre Pyspark + Spark + SQL + DF
'''
df_customers_parq = spark.read.parquet("/mnt/processing/customers_RJ.parquet")
df_customers_parq.createOrReplaceTempView("CustomersParquetTableByState")
df_customers_by_state_parq = spark.sql("select * from CustomersParquetTableByState where customer_state='RJ'")
display(df_customers_by_state_parq)
'''

# COMMAND ----------

# DBTITLE 1,Escrevendo o processo 'CSV'  - Container Curated 
# Criando o dataframe vindo de um arquivo parquet do container Processing

df_customers_parq = spark.read.parquet("/mnt/processing/customers_RJ.parquet")

# COMMAND ----------

# Convertendo o dataframe e jogando este dentro do container Curated 

df_customers_parq.write.mode('overwrite').option('header','True').option('delimiter', ',').csv('/mnt/curated/customers_RJ.csv')

# COMMAND ----------

df_RJ = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/curated/customers_RJ.csv")


# COMMAND ----------

df_RJ.display()

# COMMAND ----------

# DBTITLE 1,Criando uma tabela  do arquivo 'csv' - Container Curated
# MAGIC %sql
# MAGIC -- Filtered Curated CSV
# MAGIC CREATE TABLE IF NOT EXISTS customers_db.customers_RJ_csv 
# MAGIC USING CSV
# MAGIC LOCATION '/mnt/curated/customers_RJ.csv'
# MAGIC OPTIONS (header "true", inferSchema "true")

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE customers_db.customers_RJ_csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_db.customers_RJ_csv 

# COMMAND ----------

# MAGIC %sql
# MAGIC REFRESH TABLE customers_db.customers_pqt

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS ID_PESSOAS_SP
# MAGIC FROM customers_db.customers_pqt
# MAGIC WHERE customer_state = 'SP'
# MAGIC AND customer_city = 'sao paulo';
