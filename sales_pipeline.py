##### Libraries
from pyspark.sql.functions import substring, length, col, expr
from pyspark.sql.types import IntegerType,BooleanType,DateType,FloatType
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkContext, SparkConf
from google.cloud import bigquery

##### Constants
bucket_name="abinbev_business_case_bkt"
path_file1=f"gs://{bucket_name}/channel_group.csv"
path_file2=f"gs://{bucket_name}/sales.csv"

##### Spark Init
conf = SparkConf().setAppName('fa_plano_producao').set("spark.sql.sources.partitionOverwriteMode", "dynamic") \
                                      .set("hive.exec.dynamic.partition", "true") \
                                      .set("spark.sql.debug.maxToStringFields", 1000) \
                                      .set("hive.exec.dynamic.partition.mode", "nonstrict") \
                                      .set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false") \
                                      .set('temporaryGcsBucket', bucket_name) \
                                      .set("viewsEnabled","true")\
                                      .set("materializationExpirationTimeInMinutes","20") 
    
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
sc=spark.sparkContext
sqlContext = SQLContext(sc)
spark.conf.set('temporaryGcsBucket', bucket_name)

##### Load Data
channel_source=spark.read.csv(path_file1, header=True)
sales_source=spark.read.format("csv").option("delimiter", "\t")\
         .option("header",True)\
         .option("encoding", "UTF-16")\
         .load(path_file2)

##### Data Clealing

#Drop null rows
sales_source = sales_source.dropna()
#Remove PERIOD especial character
sales_source = sales_source.withColumn("PERIOD",expr("substring(PERIOD, 1, length(PERIOD)-1)"))

#Rename columns
sales_source = sales_source.withColumnRenamed("CE_BRAND_FLVR","BRAND_ID") \
                           .withColumnRenamed("Btlr_Org_LVL_C_Desc","REGION") \
                           .withColumnRenamed("TRADE_CHNL_DESC","TRADE_GROUP") \
                           .withColumnRenamed("$ Volume","VOLUME") \
                           .withColumnRenamed("TSR_PCKG_NM","PKG_NM") \
                           .withColumnRenamed("Pkg_Cat_Desc","PKG_CAT_DESC") 

#Change columns data types
sales_source = sales_source.withColumn("VOLUME", sales_source.VOLUME.cast(FloatType())) \
                           .withColumn("BRAND_ID", sales_source.BRAND_ID.cast(IntegerType())) \
                           .withColumn("YEAR", sales_source.YEAR.cast(IntegerType())) \
                           .withColumn("MONTH", sales_source.MONTH.cast(IntegerType())) \
                           .withColumn("PERIOD", sales_source.PERIOD.cast(IntegerType())) 

#Remove columns where VOLUME of sales is less or equal to 0
sales_source = sales_source.where("VOLUME > 0")

#Merge sales and channel data
sales = sales_source.join(channel_source, sales_source.TRADE_GROUP == channel_source.TRADE_CHNL_DESC, "inner")
sales = sales.select("DATE", "BRAND_ID", "BRAND_NM", "REGION", "TRADE_GROUP", "CHNL_GROUP", "TRADE_GROUP_DESC", "TRADE_TYPE_DESC", "PKG_NM", "PKG_CAT", "PKG_CAT_DESC",  "VOLUME", "YEAR", "MONTH", "PERIOD")

##### Table Creation

# Create temporary view
sales.createOrReplaceTempView("sales")

#Brand dimension
brand= spark.sql("SELECT DISTINCT BRAND_ID, BRAND_NM FROM sales GROUP BY BRAND_ID, BRAND_NM ORDER BY BRAND_ID")
#Create table in BQ
brand.write.format('bigquery') \
          .option('table', 'abinbev-business-case.DIMENSION.brand') \
          .mode('overwrite') \
          .save()

#PKG dimension
pkg = spark.sql("SELECT DISTINCT PKG_NM, PKG_CAT, PKG_CAT_DESC  FROM sales GROUP BY PKG_NM, PKG_CAT, PKG_CAT_DESC ORDER BY PKG_NM")
#Create table in BQ
pkg.write.format('bigquery') \
          .option('table', 'abinbev-business-case.DIMENSION.package') \
          .mode('overwrite') \
          .save()

#Trade Group dimension
tg = spark.sql("SELECT DISTINCT TRADE_GROUP, CHNL_GROUP, TRADE_GROUP_DESC, TRADE_TYPE_DESC FROM sales GROUP BY TRADE_GROUP, CHNL_GROUP, TRADE_GROUP_DESC, TRADE_TYPE_DESC ORDER BY TRADE_GROUP")
tg.write.format('bigquery') \
          .option('table', 'abinbev-business-case.DIMENSION.trade_group') \
          .mode('overwrite') \
          .save()

#Sales fact
sales_fact = spark.sql("SELECT DATE, BRAND_NM, REGION, TRADE_GROUP, PKG_NM, VOLUME, YEAR, MONTH, PERIOD FROM sales")
#Create table in BQ
sales_fact.write.format('bigquery') \
          .option('table', 'abinbev-business-case.FACT.sales') \
          .mode('overwrite') \
          .save()

##### Data Report Views

# Create temporary view
sales_fact.createOrReplaceTempView("sales_fact")
#Summary Tables
sales_fact = spark.read.format('bigquery')\
    .option('table','abinbev-business-case.FACT.sales')\
    .load()

# What are the Top 3 Trade Groups (TRADE_GROUP_DESC) for each Region (Btlr_Org_LVL_C_Desc) in sales ($ Volume)?
report1 = spark.sql("""
    SELECT  
      TRADE_GROUP,
      REGION,
      SALES
    FROM (
      SELECT 
        TRADE_GROUP,
        REGION, 
        SUM(VOLUME) AS SALES,
        RANK() OVER(PARTITION BY REGION ORDER BY SUM(VOLUME) DESC) AS TOP_SALES
      FROM 
        sales_fact
      GROUP BY REGION, TRADE_GROUP 
    )temp
    WHERE TOP_SALES < 4
""")

report1.write.format('bigquery') \
       .option('table', 'abinbev-business-case.SUMMARY_VIEWS.top3_trade_groups_per_region') \
       .mode('overwrite') \
       .save()

#How much sales ($ Volume) each brand (BRAND_NM) achieved per month?
report2 = spark.sql("""
    SELECT 
      BRAND_NM, 
      MONTH,
      SUM(VOLUME) as SALES
    FROM 
      sales_fact
    GROUP BY
      BRAND_NM, 
      MONTH
""")

report2.write.format('bigquery') \
       .option('table', 'abinbev-business-case.SUMMARY_VIEWS.brand_sales_per_month') \
       .mode('overwrite') \
       .save()

#Which are the lowest brand (BRAND_NM) in sales ($ Volume) for each region (Btlr_Org_LVL_C_Desc)?
report3 = spark.sql("""
    SELECT 
      BRAND_NM, 
      REGION,
      SALES
    FROM (
      SELECT 
        BRAND_NM,
        REGION, 
        SUM(VOLUME) AS SALES,
        RANK() OVER(PARTITION BY REGION ORDER BY SUM(VOLUME) ASC) AS SALES_RANK
      FROM 
        sales_fact
      GROUP BY BRAND_NM, REGION 
    )temp
    WHERE SALES_RANK = 1
""")

report3.write.format('bigquery') \
       .option('table', 'abinbev-business-case.SUMMARY_VIEWS.lowest_brand_sales_per_region') \
       .mode('overwrite') \
       .save()
















