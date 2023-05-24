#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#Load, Transform, Persist Pipeline

#1-mount the data lakes

#2-loads csvs from landing data lake

#3-convert csvs to parquet and move then to processing data lake

#4-create sql database

#5-create tables based on parquet format files

#6-specific analysis wil be moved to curated data lake and then loaded into sql tables

#7-powerbi application reads directly from sql tables at databricks rest api service


# In[ ]:


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


# In[ ]:


# Configurações para montar os containers do Data Lake Storage no Databricks
'''
configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": "60c2523e-a4b9-488b-8949-c57705ae2c18",  
          "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="olist_scope",key="olist-secret"),
          "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/2da3dd61-f5b3-42a5-8b14-880fa1054f9a/oauth2/token"} 





'''


# In[ ]:


# Montando Landing
'''
dbutils.fs.mount(
  source = "abfss://landing@oliststorageaccount.dfs.core.windows.net/", 
  mount_point = "/mnt/landing",                                         
  extra_configs = configs)                                              
'''


# In[ ]:


# Verificando o Landing

'''
dbutils.fs.ls("/mnt/landing/")
'''


# In[ ]:


# Montando o Processing
'''
dbutils.fs.mount(
  source = "abfss://processing@oliststorageaccount.dfs.core.windows.net/", 
  mount_point = "/mnt/processing",                                         
  extra_configs = configs)  
'''


# In[ ]:


# Verificando o Processing
'''
dbutils.fs.ls("/mnt/processing/")
'''


# In[ ]:


# Montando o Curated 
'''
dbutils.fs.mount(
  source = "abfss://curated@oliststorageaccount.dfs.core.windows.net/", 
  mount_point = "/mnt/curated",                                         
  extra_configs = configs)  
  '''


# In[ ]:


# Verificando o Processing
'''
dbutils.fs.ls("/mnt/curated/")
'''


# In[ ]:


'''
dbutils.fs.unmount("/mnt/name_container")

'''


# In[ ]:


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


# In[ ]:


df_sellers.display()


# In[ ]:


# Mostrando o Schema (Estrutura de Dados)

df_customers.printSchema()


# In[ ]:


# Criando uma view temporária do arquvio customers 

df_customers.createOrReplaceTempView('customers_tempview')


# In[ ]:


# Ver a Temp View de outras maneiras com o spark:

# spark.sql('SELECT * FROM customers_tempview').show(df_customers.count(), False) # Mostrar todas as linhas padrão do .show() é 20
# spark.sql('SELECT * FROM customers_tempview').display()


# In[ ]:


get_ipython().run_line_magic('sql', '')

SELECT * FROM customers_tempview;


# In[ ]:


get_ipython().run_line_magic('sql', '')
CREATE DATABASE IF NOT EXISTS customers_db;


# In[ ]:


get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS customers_db.customers 
USING CSV
LOCATION '/mnt/landing/dbo.olist_customers_dataset.csv'
OPTIONS (header "true", inferSchema "true");


# In[ ]:



get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS customers_db.geolocation 
USING CSV
LOCATION '/mnt/landing/dbo.olist_geolocation_dataset.csv'
OPTIONS (header "true", inferSchema "true")


# In[ ]:



get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS customers_db.order_items 
USING CSV
LOCATION '/mnt/landing/dbo.olist_order_items_dataset.csv'
OPTIONS (header "true", inferSchema "true")


# In[ ]:



get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS customers_db.order_payments 
USING CSV
LOCATION '/mnt/landing/dbo.olist_order_payments_dataset.csv'
OPTIONS (header "true", inferSchema "true")


# In[ ]:



get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS customers_db.order_reviews 
USING CSV
LOCATION '/mnt/landing/dbo.olist_order_reviews_dataset.csv'
OPTIONS (header "true", inferSchema "true")


# In[ ]:



get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS customers_db.orders 
USING CSV
LOCATION '/mnt/landing/dbo.olist_orders_dataset.csv'
OPTIONS (header "true", inferSchema "true")


# In[ ]:



get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS customers_db.sellers 
USING CSV
LOCATION '/mnt/landing/dbo.olist_sellers_dataset.csv'
OPTIONS (header "true", inferSchema "true")


# In[ ]:



get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS customers_db.product_category_name_translation 
USING CSV
LOCATION '/mnt/landing/dbo.product_category_name_translation.csv'
OPTIONS (header "true", inferSchema "true")


# In[ ]:


# Exemplo para deletar as tabelas da base de dados 'customers_db'

'''
%sql
DROP TABLE IF EXISTS customers_db.geolocation     
'''


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT COUNT(customer_id) FROM customers_db.customers;


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT *
FROM customers_db.customers;


# In[ ]:


get_ipython().run_line_magic('sql', '')
DESCRIBE customers_db.customers;


# In[ ]:


df_customers_sql = spark.table('customers_db.customers')

df_customers_sql.display()


# In[ ]:


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


# In[ ]:


# df_customers_sql.select(col('customer_state') == 'RJ').display()  ----> Por isso o uso do filter pois assim só nos traz True e False 

'''
from pyspark.sql.functions import col

df_customers_sql.select(col('customer_state') == 'RJ').display() 
'''


# In[ ]:


# Fução filter() + col() 
# col() para selecionar coluna e fazer filtragem, usando operadores relacionais como por exemplo

from pyspark.sql.functions import col

df_customers_sql2 = df_customers_sql.filter(col("customer_state") == "RJ") # Colocando a filtragem em um novo dataframe 


df_customers_sql2.display() # Or display(df_customers_sql2)


# In[ ]:


# df_customers = spark.read.format('csv').option('inferSchema', 'True').option('header', 'true').option('delimiter',',').load('/mnt/landing/dbo.olist_customers_dataset.csv')
# write.mode().parquet()
# Full-parquet

df_customers.write.mode("overwrite").parquet("/mnt/processing/customers.parquet")


# In[ ]:


df_geolocation.write.mode("overwrite").parquet("/mnt/processing/geolocation.parquet")
df_order_items.write.mode("overwrite").parquet("/mnt/processing/order_items.parquet")
df_order_payments.write.mode("overwrite").parquet("/mnt/processing/order_payments.parquet")
df_order_reviews.write.mode("overwrite").parquet("/mnt/processing/order_reviews.parquet")
df_orders.write.mode("overwrite").parquet("/mnt/processing/orders.parquet")
df_sellers.write.mode("overwrite").parquet("/mnt/processing/sellers.parquet")
df_product_category_name_translation.write.mode("overwrite").parquet("/mnt/processing/product_category_name_translation.parquet")


# In[ ]:


# Filtered-parquet

df_customers_sql2.write.mode('overwrite').parquet('/mnt/processing/customers_RJ.parquet')


# In[ ]:


df_customers_parq = spark.read.parquet('/mnt/processing/customers_RJ.parquet')

df_customers_parq.display()


# In[ ]:


get_ipython().run_line_magic('sql', '')
-- Full parquet -- Sem filtragem
CREATE TABLE IF NOT EXISTS customers_db.customers_pqt USING PARQUET OPTIONS (path "/mnt/processing/customers.parquet", header "true", inferSchema "true")

-- OUTRO MODO DE CRIAÇÃO: 
-- %sql
-- CREATE TABLE IF NOT EXISTS customers_db.customers_pqt
-- USING PARQUET
-- LOCATION '/mnt/processing/customers.parquet'
-- OPTIONS (header "true", inferSchema "true")


# In[ ]:




get_ipython().run_line_magic('sql', '')
-- Full parquet
CREATE TABLE IF NOT EXISTS customers_db.geolocation_pqt USING PARQUET OPTIONS (path "/mnt/processing/geolocation.parquet", header "true", inferSchema "true")


# In[ ]:




get_ipython().run_line_magic('sql', '')
-- Full parquet
CREATE TABLE IF NOT EXISTS customers_db.order_items_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_items.parquet", header "true", inferSchema "true")


# In[ ]:




get_ipython().run_line_magic('sql', '')
-- Full parquet
CREATE TABLE IF NOT EXISTS customers_db.order_payments_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_payments.parquet", header "true", inferSchema "true")



# In[ ]:



get_ipython().run_line_magic('sql', '')
-- Full parquet
CREATE TABLE IF NOT EXISTS customers_db.order_reviews_pqt USING PARQUET OPTIONS (path "/mnt/processing/order_reviews.parquet", header "true", inferSchema "true")


# In[ ]:




get_ipython().run_line_magic('sql', '')
-- Full parquet
CREATE TABLE IF NOT EXISTS customers_db.orders_pqt USING PARQUET OPTIONS (path "/mnt/processing/orders.parquet", header "true", inferSchema "true")


# In[ ]:



get_ipython().run_line_magic('sql', '')
-- Full parquet
CREATE TABLE IF NOT EXISTS customers_db.sellers_pqt USING PARQUET OPTIONS (path "/mnt/processing/sellers.parquet", header "true", inferSchema "true")



# In[ ]:



get_ipython().run_line_magic('sql', '')
-- Full parquet
CREATE TABLE IF NOT EXISTS customers_db.product_category_name_translation_pqt USING PARQUET OPTIONS (path "/mnt/processing/product_category_name_translation.parquet", header "true", inferSchema "true")



# In[ ]:


get_ipython().run_line_magic('sql', '')
-- Filtered parquet
CREATE TABLE IF NOT EXISTS customers_db.customers_RJ_pqt USING PARQUET OPTIONS (path "/mnt/processing/customers_RJ.parquet", header "true", inferSchema "true")


# In[ ]:


get_ipython().run_line_magic('sql', '')
REFRESH TABLE customers_db.customers_pqt


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT * FROM customers_db.customers_pqt


# In[ ]:


get_ipython().run_line_magic('sql', '')
REFRESH TABLE customers_db.customers_RJ_pqt


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT * FROM customers_db.customers_RJ_pqt


# In[ ]:


get_ipython().run_line_magic('sql', '')
REFRESH TABLE customers_db.customers_RJ_pqt


# In[ ]:


'''
df_customers_parq = spark.read.parquet("/mnt/processing/customers_RJ.parquet")
df_customers_parq.createOrReplaceTempView("CustomersParquetTableByState")
df_customers_by_state_parq = spark.sql("select * from CustomersParquetTableByState where customer_state='RJ'")
display(df_customers_by_state_parq)
'''


# In[ ]:


# Criando o dataframe vindo de um arquivo parquet do container Processing

df_customers_parq = spark.read.parquet("/mnt/processing/customers_RJ.parquet")


# In[ ]:


# Convertendo o dataframe e jogando este dentro do container Curated 

df_customers_parq.write.mode('overwrite').option('header','True').option('delimiter', ',').csv('/mnt/curated/customers_RJ.csv')


# In[ ]:


df_RJ = spark.read.format("csv").option("inferSchema", "true").option("header","true").option("delimiter",",").load("/mnt/curated/customers_RJ.csv")


# In[ ]:


df_RJ.display()


# In[ ]:


get_ipython().run_line_magic('sql', '')
-- Filtered Curated CSV
CREATE TABLE IF NOT EXISTS customers_db.customers_RJ_csv 
USING CSV
LOCATION '/mnt/curated/customers_RJ.csv'
OPTIONS (header "true", inferSchema "true")


# In[ ]:


get_ipython().run_line_magic('sql', '')
REFRESH TABLE customers_db.customers_RJ_csv


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT * FROM customers_db.customers_RJ_csv 


# In[ ]:


get_ipython().run_line_magic('sql', '')
REFRESH TABLE customers_db.customers_pqt


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT COUNT(*) AS ID_PESSOAS_SP
FROM customers_db.customers_pqt
WHERE customer_state = 'SP'
AND customer_city = 'sao paulo';

