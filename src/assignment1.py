# Databricks notebook source
from pyspark.sql.functions import when,col,explode_outer

# COMMAND ----------

dbutils.fs.mount(
    source= 'wasbs://containerdatabricks@db101storageaccount.blob.core.windows.net/',
    mount_point='/mnt/blob_storage1',
    extra_configs = {'fs.azure.account.key.db101storageaccount.blob.core.windows.net':'xlrBFXbXd7K4KG7009707IxVISM7Qn3Ea7OCwTYK1Ej/JpPz/bhuZt49/Cz7gIXIQSn+L4LaGUoS+AStyEN4LQ=='}
)

# COMMAND ----------

csv_df = spark.read.csv('/mnt/blob_storage1/sample_csv.csv',header=True,inferSchema=True)
display(df)
parquet_output_path = "/mnt/blob_storage1/bronze/csv/"
csv_df.write.parquet(parquet_output_path)

# COMMAND ----------

json_df = spark.read.option('multiline','true').json('/mnt/blob_storage1/sample_json_1.json')
display(df)
parquet_output_path1 = "/mnt/blob_storage1/bronze/json/"
json_df.write.parquet(parquet_output_path1)

# COMMAND ----------

paraquet_df_csv = spark.read.parquet(parquet_output_path)
# Remove Duplicates
dropduplicate_df_csv= paraquet_df_csv.dropDuplicates()
# replace lower values of column name
colupper_df_csv = dropduplicate_df_csv.toDF(*[col.title() for col in dropduplicate_df_csv.columns])
# replace null values with 0
replaced_columns_csv = colupper_df_csv.fillna('0')
replaced_columns_csv = replaced_columns.withColumn('City',when(col('City') == 'Null','0').otherwise(col('City')))\
    .withColumn('Name',when(col('Name') == 'Null','0').otherwise(col('Name')))
display(replaced_columns_csv)

# COMMAND ----------

paraquet_df_json = spark.read.parquet(parquet_output_path1)
#explode
paraquet_df_json = paraquet_df_json.withColumn('name',explode_outer('projects.name'))\
    .withColumn('status',explode_outer('projects.status'))\
    .drop('projects')

# Remove Duplicates
dropduplicate_df_json= paraquet_df_json.dropDuplicates()
# replace lower values of column name
colupper_df_json = dropduplicate_df_json.toDF(*[col.title() for col in dropduplicate_df_json.columns])
display(colupper_df_json)

# replace null values with 0
replaced_columns_json = colupper_df_json.na.fill({'id': 0,'salary': 0,'name': 0,'status':0,'department':0})
display(replaced_columns_json)

# COMMAND ----------

# writing json into silver layer
parquet_output_path = "/mnt/blob_storage1/silver/csv/"
replaced_columns_json.write.parquet(parquet_output_path)
# writing csv into silver layer
parquet_output_path1 = "/mnt/blob_storage1/silver/json/"
replaced_columns_csv.write.parquet(parquet_output_path1)

# COMMAND ----------

# join table
paraquet_df_csv = spark.read.parquet(parquet_output_path)
paraquet_df_json = spark.read.parquet(parquet_output_path1)
paraquet_df_jsons =paraquet_df_json.withColumnRenamed('Name','Names')
display(paraquet_df_jsons)
joined_table = paraquet_df_csv.join(paraquet_df_jsons,'Id','outer')
display(joined_table)

# COMMAND ----------

# loading to gold layer
join_output = "/mnt/blob_storage1/gold/"
joined_table.write.parquet(join_output)

# COMMAND ----------

joined_table = spark.read.parquet("/mnt/blob_storage1/gold/")
display(joined_table)
joined_table = joined_table.select('Id','Department','Salary','Name','Status','Names')
