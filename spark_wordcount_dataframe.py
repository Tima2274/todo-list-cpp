"""
WordCount через PySpark DataFrame API.
Более современный и оптимизированный подход.

Usage:
    python spark_wordcount_dataframe.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower


def main():
    # Инициализация SparkSession
    spark = SparkSession.builder \
        .appName("WordCount-DataFrame") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Загрузка данных
    input_path = "datasets/test_data.txt"
    output_path = "output/wordcount_dataframe"

    # Чтение файла
    df = spark.read.text(input_path)

    # Pipeline трансформаций
    word_counts_df = (df
        .select(explode(split(lower(df.value), "\\s+")).alias("word"))
        .groupBy("word")
        .count()
    )

    # Сортировка по убыванию частоты
    word_counts_sorted = word_counts_df.orderBy("count", ascending=False)

    # Сохранение результатов
    word_counts_sorted.write.mode("overwrite").csv(output_path)

    # Вывод в консоль
    print("=" * 60)
    print("WORDCOUNT RESULTS (DataFrame API)")
    print("=" * 60)
    word_counts_sorted.show(50, truncate=False)
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
