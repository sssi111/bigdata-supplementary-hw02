#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg, count, year, month, to_date, round as _round, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
import sys

def create_spark_session():    
    spark = SparkSession.builder \
        .appName("SparkYARNDataProcessing") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.executor.memory", "1g") \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.instances", "2") \
        .config("spark.sql.catalogImplementation", "hive") \
        .config("spark.sql.warehouse.dir", "hdfs://192.168.1.15:9000/user/hive/warehouse") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark

def create_sample_data(spark):    
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("product", StringType(), False),
        StructField("category", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("sale_date", StringType(), False),
    ])
    
    sample_data = [
        (1, "Laptop", "Electronics", 1200.50, 2, "2024-01-15"),
        (2, "Mouse", "Electronics", 25.99, 5, "2024-01-16"),
        (3, "Keyboard", "Electronics", 79.99, 3, "2024-01-17"),
        (4, "Monitor", "Electronics", 299.99, 1, "2024-01-18"),
        (5, "Desk Chair", "Furniture", 199.99, 2, "2024-01-19"),
        (6, "Desk", "Furniture", 399.99, 1, "2024-01-20"),
        (7, "Lamp", "Furniture", 49.99, 4, "2024-01-21"),
        (8, "Phone", "Electronics", 899.99, 1, "2024-02-01"),
        (9, "Tablet", "Electronics", 599.99, 2, "2024-02-02"),
        (10, "Headphones", "Electronics", 149.99, 3, "2024-02-03"),
        (11, "Webcam", "Electronics", 89.99, 2, "2024-02-04"),
        (12, "Bookshelf", "Furniture", 159.99, 1, "2024-02-05"),
        (13, "Office Cabinet", "Furniture", 299.99, 1, "2024-02-06"),
        (14, "USB Cable", "Electronics", 9.99, 10, "2024-03-01"),
        (15, "HDMI Cable", "Electronics", 14.99, 8, "2024-03-02"),
        (16, "Printer", "Electronics", 249.99, 1, "2024-03-03"),
        (17, "Scanner", "Electronics", 179.99, 1, "2024-03-04"),
        (18, "Standing Desk", "Furniture", 599.99, 1, "2024-03-05"),
        (19, "Ergonomic Mouse", "Electronics", 45.99, 4, "2024-03-06"),
        (20, "Monitor Arm", "Furniture", 129.99, 2, "2024-03-07"),
    ]
    
    df = spark.createDataFrame(sample_data, schema)
    
    hdfs_path = "hdfs://192.168.1.15:9000/data/sample/sales_raw"
    df.write.mode("overwrite").csv(hdfs_path, header=True)
    
    return hdfs_path

def read_data_from_hdfs(spark, hdfs_path):    
    df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
    
    df.printSchema()
    
    df.show(5, truncate=False)
    
    return df

def transform_data(df):    
    df = df.withColumn("sale_date_typed", to_date(col("sale_date"), "yyyy-MM-dd"))
    df = df.withColumn("year", year(col("sale_date_typed"))) \
           .withColumn("month", month(col("sale_date_typed")))
    df = df.withColumn("total_revenue", _round(col("amount") * col("quantity"), 2))
    df = df.withColumn(
        "price_category",
        when(col("amount") < 50, "low")
        .when(col("amount") < 200, "medium")
        .otherwise("high")
    )
    
    df.printSchema()
    
    df.select("id", "product", "amount", "quantity", "total_revenue", 
              "price_category", "year", "month").show(10, truncate=False)
    
    return df

def create_aggregated_view(spark, df):
    category_stats = df.groupBy("category") \
        .agg(
            count("id").alias("total_transactions"),
            _sum("quantity").alias("total_quantity"),
            _round(_sum("total_revenue"), 2).alias("total_revenue"),
            _round(avg("amount"), 2).alias("avg_price")
        ) \
        .orderBy(col("total_revenue").desc())
    
    category_stats.show(truncate=False)
    
    monthly_stats = df.groupBy("year", "month") \
        .agg(
            count("id").alias("total_transactions"),
            _round(_sum("total_revenue"), 2).alias("total_revenue"),
            _round(avg("total_revenue"), 2).alias("avg_transaction_value")
        ) \
        .orderBy("year", "month")
    
    monthly_stats.show(truncate=False)
    
    price_category_stats = df.groupBy("price_category") \
        .agg(
            count("id").alias("total_products"),
            _round(_sum("total_revenue"), 2).alias("total_revenue")
        ) \
        .orderBy("price_category")
    
    price_category_stats.show(truncate=False)
    
    return category_stats, monthly_stats

def save_as_hive_table(spark, df, table_name, database="spark_demo"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    spark.sql(f"USE {database}")
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    
    df.write \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .format("parquet") \
        .saveAsTable(f"{database}.{table_name}")
    
    spark.sql(f"DESCRIBE EXTENDED {database}.{table_name}").filter(
        col("col_name") == "Location"
    ).show(truncate=False)
    
    partitions = spark.sql(f"SHOW PARTITIONS {database}.{table_name}")
    partitions.show(truncate=False)
    
    return f"{database}.{table_name}"

def verify_hive_table(spark, table_name):
    df = spark.sql(f"SELECT * FROM {table_name}")
    
    top_products = spark.sql(f"""
        SELECT product, category, 
               ROUND(SUM(total_revenue), 2) as revenue,
               SUM(quantity) as total_quantity
        FROM {table_name}
        GROUP BY product, category
        ORDER BY revenue DESC
        LIMIT 5
    """)
    top_products.show(truncate=False)
    
    january_data = spark.sql(f"""
        SELECT product, total_revenue, sale_date_typed
        FROM {table_name}
        WHERE year = 2024 AND month = 1
        ORDER BY total_revenue DESC
    """)
    january_data.show(truncate=False)
    
    return True

def main():
    spark = None
    try:
        spark = create_spark_session()
        
        hdfs_path = create_sample_data(spark)
        
        df = read_data_from_hdfs(spark, hdfs_path)
        
        df_transformed = transform_data(df)
        
        category_stats, monthly_stats = create_aggregated_view(spark, df_transformed)
        
        table_name = save_as_hive_table(spark, df_transformed, 
                                        "sales_data_processed", 
                                        "spark_demo")
        
        verify_hive_table(spark, table_name)
        
        category_stats.write \
            .mode("overwrite") \
            .format("parquet") \
            .saveAsTable("spark_demo.sales_by_category")
        
        monthly_stats.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .format("parquet") \
            .saveAsTable("spark_demo.sales_by_month")

        spark.sql("SHOW TABLES IN spark_demo").show(truncate=False)
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()


