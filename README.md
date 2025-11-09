# bigdata-supplementary-hw04

Apache Spark 3.5.0 под управлением YARN для чтения, трансформации и записи данных.

## Быстрый старт

### 1. Развертывание Spark

```bash
cd bigdata-supplementary-hw02/cluster
ansible-playbook -i inventory.ini deploy-spark.yml --ask-become-pass
```

### 2. Запуск демонстрационного приложения

```bash
ssh team@192.168.1.15
sudo -u spark JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/spark/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --driver-memory 1g \
  --executor-memory 1g \
  --executor-cores 1 \
  --num-executors 2 \
  /opt/spark/scripts/spark-data-processing.py
```

### 3. Проверка через Spark SQL

```bash
sudo -u spark JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 /opt/spark/bin/spark-sql \
  --master yarn \
  --deploy-mode client \
  -e "SELECT * FROM spark_demo.sales_data_processed LIMIT 5;"
```

## Архитектура

**Компоненты:**
- Spark on YARN - управление ресурсами через YARN ResourceManager
- HDFS Integration - чтение/запись данных из HDFS кластера
- Hive Metastore Integration - работа с Hive таблицами

## Демонстрационный скрипт

Скрипт `spark-data-processing.py` выполняет полный цикл обработки данных:

### 1. Создание Spark сессии под YARN

```python
spark = SparkSession.builder \
    .appName("SparkYARNDataProcessing") \
    .master("yarn") \
    .enableHiveSupport() \
    .getOrCreate()
```

### 2. Чтение данных из HDFS

```python
df = spark.read.csv("hdfs://192.168.1.15:9000/data/sample/sales.csv", 
                    header=True, inferSchema=True)
```

### 3. Трансформации данных

```python
df = df.withColumn("sale_date_typed", to_date(col("sale_date")))
df = df.withColumn("amount", col("amount").cast("double"))
df = df.withColumn("year", year(col("sale_date_typed")))
df = df.withColumn("month", month(col("sale_date_typed")))
df = df.withColumn("total_revenue", col("amount") * col("quantity"))
df = df.withColumn("price_category",
    when(col("amount") < 50, "low")
    .when(col("amount") < 200, "medium")
    .otherwise("high"))
category_stats = df.groupBy("category").agg(
    sum("total_revenue").alias("revenue"),
    avg("amount").alias("avg_price"),
    count("id").alias("count")
)
```

### 4. Сохранение с партиционированием в Hive таблицу

```python
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .format("parquet") \
    .saveAsTable("spark_demo.sales_data_processed")
```

### 5. Результат

Созданные таблицы:
- `spark_demo.sales_data_processed` - основная таблица с партициями `year=2024/month={1,2,3}`
- `spark_demo.sales_by_category` - агрегация по категориям
- `spark_demo.sales_by_month` - агрегация по месяцам

## Мониторинг

### SSH туннель для доступа к веб-интерфейсам

```bash
ssh -J team@176.109.91.5 \
    -L 4040:127.0.0.1:4040 \
    -L 8088:127.0.0.1:8088 \
    team@192.168.1.15
```

Откройте в браузере:
- **Spark Application UI**: http://localhost:4040 (активное приложение)
- **YARN ResourceManager**: http://localhost:8088 (все приложения)

## Конфигурация

### Файлы конфигурации:
- `cluster/config/spark-env.sh.j2` - переменные окружения
- `cluster/config/spark-defaults.conf.j2` - конфигурация по умолчанию
- `cluster/vars.yml` - параметры развертывания

### Ansible Playbooks:
- `deploy-spark.yml` - развертывание Spark на кластере

### Скрипты:
- `scripts/spark-data-processing.py` - демонстрационный скрипт
- `scripts/check-spark.sh` - проверка установки

## Команда

ВШЭ СПб, ПМИ, 4 курс
* Нейков Даниил
* Панов Андрей
* Мацкевич Валерий
