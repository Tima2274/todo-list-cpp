"""
Анализ веб-логов с помощью PySpark.
Аналог задания 2 из практики 1.6 (Hadoop MapReduce).

Задачи:
1. Топ-10 IP-адресов по числу запросов;
2. Распределение HTTP status codes.

Usage:
    python spark_weblog_analysis.py
"""

import re
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, count as spark_count


def parse_log_line(line: str) -> tuple:
    """
    Парсит одну строку веб-лога.
    
    Returns:
        Кортеж (ip, method, path, status, size) или None при ошибке.
    """
    pattern = r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+(\S+)\s+\S+"\s+(\d+)\s+(\d+)'
    match = re.match(pattern, line)
    if match:
        return (match.group(1), match.group(3), match.group(4),
                int(match.group(5)), int(match.group(6)))
    return None


def main():
    # Инициализация — SparkSession создаёт SparkContext автоматически
    spark = SparkSession.builder \
        .appName("WebLog-Analysis") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    input_path = "datasets/web_logs.txt"
    output_path = "output/weblog_analysis"

    # ========================================
    # Вариант 1: RDD API
    # ========================================
    print("\n" + "=" * 60)
    print("WEB LOG ANALYSIS (RDD API)")
    print("=" * 60)

    rdd = sc.textFile(input_path)

    # Парсинг строк
    parsed_rdd = rdd.map(parse_log_line).filter(lambda x: x is not None)

    # Топ-10 IP-адресов
    ip_counts = (parsed_rdd
        .map(lambda x: (x[0], 1))
        .reduceByKey(lambda a, b: a + b)
        .top(10, key=lambda x: x[1])
    )

    print("\nTop 10 IP addresses:")
    print(f"{'IP':<25} {'Requests':>10}")
    print("-" * 35)
    for ip, count in ip_counts:
        print(f"{ip:<25} {count:>10}")

    # Распределение status codes
    status_counts = (parsed_rdd
        .map(lambda x: (x[3], 1))
        .reduceByKey(lambda a, b: a + b)
        .collect()
    )

    print("\nHTTP Status Code Distribution:")
    print(f"{'Status':<10} {'Count':>10}")
    print("-" * 20)
    for status, count in sorted(status_counts, key=lambda x: x[0]):
        print(f"{status:<10} {count:>10}")

    # ========================================
    # Вариант 2: DataFrame API
    # ========================================
    print("\n" + "=" * 60)
    print("WEB LOG ANALYSIS (DataFrame API)")
    print("=" * 60)

    df = spark.read.text(input_path)

    # Извлечение полей через регулярные выражения
    log_df = df.select(
        regexp_extract(col("value"), r'^(\S+)', 1).alias("ip"),
        regexp_extract(col("value"), r'"(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH)\s+', 1).alias("method"),
        regexp_extract(col("value"), r'"(GET|POST|PUT|DELETE|HEAD|OPTIONS|PATCH)\s+(\S+)', 2).alias("path"),
        regexp_extract(col("value"), r'"\s+(\d{3})\s+', 1).cast("int").alias("status"),
        regexp_extract(col("value"), r'"\s+\d{3}\s+(\d+)', 1).cast("int").alias("size")
    )

    # Топ-10 IP
    top_ips_df = (log_df
        .groupBy("ip")
        .agg(spark_count("*").alias("requests"))
        .orderBy(col("requests").desc())
        .limit(10)
    )

    print("\nTop 10 IP addresses:")
    top_ips_df.show(truncate=False)

    # Распределение status codes
    status_df = (log_df
        .groupBy("status")
        .agg(spark_count("*").alias("count"))
        .orderBy("status")
    )

    print("HTTP Status Code Distribution:")
    status_df.show()

    # Дополнительные метрики
    print("\nAdditional Metrics:")
    print(f"  Total requests: {log_df.count()}")
    print(f"  Unique IPs: {log_df.select('ip').distinct().count()}")
    print(f"  Unique paths: {log_df.select('path').distinct().count()}")

    # Средний размер ответа
    avg_size = log_df.agg({"size": "avg"}).collect()[0][0]
    print(f"  Average response size: {avg_size:.0f} bytes")

    # Сохранение результатов
    top_ips_df.write.mode("overwrite").csv(f"{output_path}/top_ips")
    status_df.write.mode("overwrite").csv(f"{output_path}/status_distribution")

    sc.stop()
    spark.stop()


if __name__ == "__main__":
    main()
