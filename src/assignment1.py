# Databricks notebook source
# DBTITLE 1,import functions
from pyspark.sql.functions import when,col,explode_outer

# COMMAND ----------

# DBTITLE 1,read csv file & write in parquet format
def read_write_csv(read_path_csv,parquet_output_path_bronze):
    csv_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(read_path_csv)
    display(df)
    csv_df.write.parquet(parquet_output_path_bronze)

# COMMAND ----------

# DBTITLE 1,read json file & write in parquet format
def read_write_json(read_path_json,parquet_output_path_bronze):
    json_df = spark.read.option('multiline','true').json(read_path_json)
    display(df)
    json_df.write.parquet(parquet_output_path_bronzejson)

# COMMAND ----------

# DBTITLE 1,perform remove,replace lower_case column ,replace null in csv
def read_csv_bronz(parquet_output_path_bronze):
    paraquet_df_csv = spark.read.parquet(parquet_output_path_bronze)
    return paraquet_df_csv

    # Remove Duplicates
def dropduplicate_csv(paraquet_df_csv):    
    dropduplicate_df_csv= paraquet_df_csv.dropDuplicates()
    return dropduplicate_df_csv

    # replace lower values of column name
def replace_col_value_csv(dropduplicate_df_csv):
    colupper_df_csv = dropduplicate_df_csv.toDF(*[col.title() for col in dropduplicate_df_csv.columns])
    return colupper_df_csv
    
    # replace null values with 0
def replace_null(colupper_df_csv):
    replaced_columns_csv = colupper_df_csv.fillna('0')
    replaced_columns_csv = replaced_columns.withColumn('City',when(col('City') == 'Null','0').otherwise(col('City')))\
        .withColumn('Name',when(col('Name') == 'Null','0').otherwise(col('Name')))
    display(replaced_columns_csv)
    return replaced_columns_csv

# COMMAND ----------

# DBTITLE 1,perform remove,replace lower_case column ,replace null in json
def read_json_bronz(parquet_output_path_bronzejson):
    paraquet_df_json = spark.read.parquet(parquet_output_path_bronzejson)
    return paraquet_df_json
    
    #explode
def explode_json(paraquet_df_json):
    paraquet_df_json = paraquet_df_json.withColumn('name',explode_outer('projects.name'))\
        .withColumn('status',explode_outer('projects.status'))\
        .drop('projects')
        return paraquet_df_json

    # Remove Duplicates
def dropduplicate_json(paraquet_df_json):
    dropduplicate_df_json= paraquet_df_json.dropDuplicates()
    return dropduplicate_df_json

    # replace lower values of column name
def replace_col_value_json(dropduplicate_df_json):
    colupper_df_json = dropduplicate_df_json.toDF(*[col.title() for col in dropduplicate_df_json.columns])
    display(colupper_df_json)
    return colupper_df_json

    # replace null values with 0
def replace_null(colupper_df_json):
    replaced_columns_json = colupper_df_json.na.fill({'id': 0,'salary': 0,'name': 0,'status':0,'department':0})
    display(replaced_columns_json)
    return replaced_columns_json

# COMMAND ----------

# DBTITLE 1,writing into silver layer
def write_to_silverlayer_json(parquet_output_path_silvercsv,replaced_columns_json):
    # writing json into silver layer
    replaced_columns_json.write.parquet(parquet_output_path_silvercsv)
    
def write_to_silverlayer_csv(parquet_output_path_silverjson,replaced_columns_csv):
    # writing csv into silver layer
    replaced_columns_csv.write.parquet(parquet_output_path_silverjson)

# COMMAND ----------

# DBTITLE 1,join table
def join_table(parquet_output_path_silverjcsv,parquet_output_path_silverjson):
    # join table
    paraquet_df_csv = spark.read.parquet(parquet_output_path_silvercsv)
    paraquet_df_json = spark.read.parquet(parquet_output_path_silverjson)
    paraquet_df_jsons =paraquet_df_json.withColumnRenamed('Name','Names')
    display(paraquet_df_jsons)
    joined_table = paraquet_df_csv.join(paraquet_df_jsons,'Id','outer')
    display(joined_table)
    return joined_table

# COMMAND ----------

# DBTITLE 1,loading into gold layer
def write_to_goldlayer(join_output_path,joined_table):
    # loading to gold layer
    joined_table.write.parquet(join_output_path)

# COMMAND ----------

# DBTITLE 1,removing the rows
def remove_rows(join_output_path):
    joined_table = spark.read.parquet(join_output_path)
    ids_to_delete = [31, 40, 7, 15]
    filtered_df =joined_table.filter(~joined_table["ID"].isin(ids_to_delete))
    display(filtered_df)

# COMMAND ----------

# DBTITLE 1,mount data
dbutils.fs.mount(
    source= 'wasbs://containerdatabricks@db101storageaccount.blob.core.windows.net/',
    mount_point='/mnt/blob_storage1',
    extra_configs = {'fs.azure.account.key.db101storageaccount.blob.core.windows.net':'xlrBFXbXd7K4KG7009707IxVISM7Qn3Ea7OCwTYK1Ej/JpPz/bhuZt49/Cz7gIXIQSn+L4LaGUoS+AStyEN4LQ=='}
)

# COMMAND ----------

# DBTITLE 1,calling functions
read_path_csv = "/mnt/blob_storage1/sample_csv.csv"
parquet_output_path_bronze = "/mnt/blob_storage1/bronze/csv/"
read_path_json = '/mnt/blob_storage1/sample_json_1.json'
parquet_output_path_bronzejson = "/mnt/blob_storage1/bronze/json/"
join_output_path = "/mnt/blob_storage1/gold/"
parquet_output_path_silvercsv = "/mnt/blob_storage1/silver/csv/"
parquet_output_path_silverjson = "/mnt/blob_storage1/silver/json/"

read_write_csv(read_path_csv,parquet_output_path_bronze)
read_write_json(read_path_json,parquet_output_path_bronze)

# read csv file from bronze layer
paraquet_df_csv = read_csv_bronz(parquet_output_path_bronze)

# Remove Duplicates in csv function
dropduplicate_df_csv=dropduplicate_csv(paraquet_df_csv)

# replace lower values of column name
colupper_df_csv = replace_col_value_csv(dropduplicate_df_csv)

# replace null values with 0
replaced_columns_csv= replace_null(colupper_df_csv)

# read json from bronze layer
paraquet_df_json=read_json_bronz(parquet_output_path_bronzejson)

# explode function
paraquet_df_json=explode_json(paraquet_df_json)

#Remove Duplicates in json function
dropduplicate_df_json= dropduplicate_json(paraquet_df_json)

#  replace lower values of column name json function
colupper_df_json=replace_col_value_json(dropduplicate_df_json)

# replace null values with 0 function
replaced_columns_json = replace_null(colupper_df_json)

# write to silver layer csv function
write_to_silverlayer_csv(parquet_output_path_silvercsv,replaced_columns_csv)

# write to silver layer json function
write_to_silverlayer_json(parquet_output_path_silvercsv,replaced_columns_json)

# join csv and json files function
join_table(parquet_output_path_silverjcsv,parquet_output_path_silverjson)

# write to gold layer function
write_to_goldlayer(join_output_path,joined_table)

# remove rows fuction
remove_rows(join_output_path)
