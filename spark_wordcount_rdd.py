"""
WordCount через PySpark RDD API.
Аналог Hadoop MapReduce WordCount из практики 1.6.

Usage:
    python spark_wordcount_rdd.py
"""

from pyspark import SparkContext
from pyspark.sql import SparkSession


def main():
    # Создаём SparkSession (для совместимости с DataFrame), затем берём sc
    spark = SparkSession.builder \
        .appName("WordCount-RDD") \
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext

    # Загрузка данных
    input_path = "datasets/test_data.txt"
    output_path = "output/wordcount_rdd"

    # RDD pipeline
    rdd = sc.textFile(input_path)

    word_counts = (rdd
        .flatMap(lambda line: line.split(" "))           # Map: разбивка на слова
        .map(lambda word: (word.lower().strip('.,!?;:"'), 1))  # Map: (слово, 1)
        .reduceByKey(lambda a, b: a + b)                 # Reduce: суммирование
    )

    # Сохранение результатов
    word_counts.saveAsTextFile(output_path)

    # Вывод в консоль
    print("=" * 60)
    print("WORDCOUNT RESULTS (RDD API)")
    print("=" * 60)
    for word, count in word_counts.collect():
        print(f"{word:<20} {count:>10}")
    print("=" * 60)

    sc.stop()
    spark.stop()


if __name__ == "__main__":
    main()
