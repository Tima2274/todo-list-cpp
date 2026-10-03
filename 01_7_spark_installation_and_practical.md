# Практика 1.7 — Установка и настройка Apache Spark


## Цель работы

В практике 1.6 вы разворачивали кластер Hadoop и писали MapReduce-программы на Java. Но у Hadoop MapReduce есть серьёзный недостаток — каждый шаг записывает промежуточные результаты на диск, что сильно замедляет вычисления.

Apache Spark создан для решения этой проблемы. Он хранит данные в оперативной памяти, что делает его в десятки раз быстрее. В этой практике вы:

1. Научитесь разворачивать кластер Apache Spark;
2. Напишете те же задачи, что и в Hadoop, но на PySpark (Python);
3. Сравните подходы Hadoop MapReduce и Spark;
4. Изучите механизм кэширования — ключевое преимущество Spark.

---

## 1. Теория: почему Spark быстрее Hadoop

### 1.1 Проблема Hadoop MapReduce

Вспомните, как работает Hadoop MapReduce из практики 1.6:

```
Входные данные → Map → (запись на диск) → Shuffle → (запись на диск) → Reduce → Результат
```

Каждый этап записывает промежуточные результаты на диск. Если вам нужно выполнить итеративный алгоритм (например, машинное обучение), каждый проход по данным — это новый Job с записями на диск. Это медленно.

### 1.2 Решение: Apache Spark

Spark решает эту проблему двумя способами:

**1. In-memory вычисления**

```
Hadoop:  Map → ДИСК → Shuffle → ДИСК → Reduce     (медленно)
Spark:   Map → ПАМЯТЬ → Shuffle → ПАМЯТЬ → Reduce   (быстро)
```

Spark хранит данные в оперативной памяти в виде **RDD** (Resilient Distributed Dataset) — отказоустойчивого распределённого набора данных. Результат Map-фазы не пишется на диск, а остаётся в памяти и сразу передаётся дальше.

**2. Ленивые вычисления (Lazy Evaluation)**

Spark не выполняет команды сразу. Он строит **DAG** (направленный ациклический граф) трансформаций и оптимизирует его перед выполнением. Это позволяет:
- Объединять несколько операций в одну;
- Избегать лишних записей на диск;
- Кэшировать промежуточные результаты.

### 1.3 Архитектура Spark

Spark работает по модели Master-Slave, аналогично Hadoop:

| Компонент | Роль | Аналог в Hadoop |
|-----------|------|-----------------|
| **Driver** | Запускает main(), создаёт задачи | ApplicationMaster |
| **Executor** | Выполняет задачи на ноде, хранит данные в памяти | NodeManager + задачи |
| **Cluster Manager** | Выделяет ресурсы (Standalone, YARN, Mesos) | YARN ResourceManager |

**Ключевое отличие:** Executor'ы Spark хранят данные в памяти и переиспользуют их для нескольких задач. В Hadoop каждый Job читает данные заново с диска.

### 1.4 Два уровня API

Spark предоставляет два уровня работы с данными:

**RDD API** — низкий уровень, аналог MapReduce. Вы работаете с распределёнными коллекциями и применяете функции `map`, `filter`, `reduceByKey`. Это полезно для понимания принципов работы Spark.

**DataFrame API** — высокий уровень, аналог SQL-таблиц. Spark автоматически оптимизирует запросы через Catalyst Optimizer. Это то, что используется в реальной работе.

В этой практике мы пройдём оба подхода на одних и тех же примерах, чтобы вы видели разницу.

---

## 2. Установка и запуск

### 2.1 Развёртывание через Docker Compose

Как и в практике 1.6, мы используем Docker. Файл `docker-compose.yml` уже создан в директории практики.

Запустите кластер:

```bash
docker-compose up -d
sleep 10
```

Откройте `http://localhost:8080` — вы увидите Spark Master UI. Если всё настроено правильно, там будет 1 подключённый Worker.


### 2.2 Локальный режим (для разработки)

Для написания и отладки кода не обязательно запускать Docker. PySpark можно запустить локально:

```bash
pyspark --master local[*]
```

Флаг `local[*]` означает, что Spark запустится в одном процессе, но будет использовать все доступные ядра процессора. Именно этот режим мы будем использовать в примерах ниже.

---

## 3. Практика 1: WordCount

### 3.1 Что мы делаем

Возьмём задачу подсчёта частоты слов из практики 1.6 и реализуем её на PySpark двумя способами. Входные данные — файл `datasets/test_data.txt` (скопирован из 1.6):

```
Hello World Hello Hadoop
Hello HDFS Hello MapReduce
Hadoop YARN is great
MapReduce processes data
Hadoop HDFS stores data
YARN manages resources
Hello Big Data
Hadoop is distributed
MapReduce is parallel
HDFS is reliable
```

### 3.2 Вариант A: RDD API

RDD API — это прямой аналог Hadoop MapReduce. Каждая операция имеет свой эквивалент:

| Hadoop MapReduce | PySpark RDD | Что делает |
|-----------------|-------------|------------|
| Mapper | `map` / `flatMap` | Преобразует каждую запись |
| Shuffle + group | `groupByKey` / `reduceByKey` | Группирует по ключу |
| Reducer | `reduceByKey` | Агрегирует значения |

Создайте файл `spark_wordcount_rdd.py`:

```python
from pyspark import SparkContext
from pyspark.sql import SparkSession


def main():
    # SparkSession создаёт SparkContext автоматически
    spark = SparkSession.builder \
        .appName("WordCount-RDD") \
        .master("local[*]") \
        .getOrCreate()
    sc = spark.sparkContext

    # 2. Загружаем данные — одна RDD на каждую строку файла
    rdd = sc.textFile("datasets/test_data.txt")

    # 3. Цепочка трансформаций
    word_counts = (rdd
        .flatMap(lambda line: line.split(" "))       # 3a. Разбиваем строки на слова
        .map(lambda word: (word.lower().strip('.,!?;:"'), 1))  # 3b. Делаем (слово, 1)
        .reduceByKey(lambda a, b: a + b)              # 3c. Суммируем по словам
    )

    # 4. Действие — запускаем вычисления и выводим результат
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
```

**Разбор по шагам:**

1. `textFile()` — загружает файл. Это **действие** (запускает чтение с диска).
2. `flatMap()` — **трансформация**. Каждая строка разбивается на слова. `flatMap` отличается от `map` тем, что возвращает несколько результатов на вход (одна строка → много слов).
3. `map()` — **трансформация**. Каждое слово оборачивается в кортеж `(слово, 1)`.
4. `reduceByKey()` — **трансформация**. Группирует по первому элементу кортежа (слово) и применяет функцию сложения ко второму элементу (1).
5. `collect()` — **действие**. Загружает все результаты в память драйвера и выводит на экран.

**Важно:** трансформации ленивые — они не выполняются сразу. Вычисление начнётся только при вызове действия (`collect`, `saveAsTextFile` и т.д.).

Запуск:

```bash
python spark_wordcount_rdd.py
```

### 3.3 Вариант B: DataFrame API

DataFrame API — это современный подход. Данные представляются как таблицы с именованными колонками, а не как кортежи.

Создайте файл `spark_wordcount_dataframe.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower


def main():
    # 1. Создаём SparkSession — точка входа для DataFrame API
    spark = SparkSession.builder \
        .appName("WordCount-DataFrame") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 2. Загружаем данные — каждая строка файла становится строкой DataFrame
    df = spark.read.text("datasets/test_data.txt")

    # 3. Цепочка трансформаций
    word_counts_df = (df
        .select(explode(split(lower(df.value), "\\s+")).alias("word"))  # Разбиваем и flattening
        .groupBy("word")                                                # Группируем по слову
        .count()                                                        # Считаем
    )

    # 4. Сортируем и выводим
    word_counts_sorted = word_counts_df.orderBy("count", ascending=False)
    word_counts_sorted.show(50, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
```

**Разбор по шагам:**

1. `spark.read.text()` — загружает файл. Результат — DataFrame с одной колонкой `value`.
2. `split(value, "\\s+")` — разбивает строку по пробелам → массив слов.
3. `explode(...)` — превращает массив в отдельные строки (одно слово — одна строка).
4. `groupBy("word").count()` — группирует по колонке `word` и считает количество записей в каждой группе.

Запуск:

```bash
python spark_wordcount_dataframe.py
```

Результат будет таким же, как в RDD API, но код короче и читаемее.

### 3.4 Что мы узнали

| Аспект | RDD API | DataFrame API |
|--------|---------|---------------|
| Модель данных | Кортежи `(word, 1)` | Таблица с колонками |
| Оптимизация | Нет, Spark выполняет как есть | Catalyst Optimizer строит оптимальный план |
| Скорость | Медленнее | Быстрее (обычно в 10-100 раз) |
| Когда использовать | Нужен полный контроль над вычислениями | Большинство задач |

---

## 4. Практика 2: Анализ веб-логов

### 4.1 Что мы делаем

Возьмём задачу анализа веб-логов из практики 1.6. Файл `datasets/web_logs.txt` содержит 1000 записей в формате Common Log Format:

```
192.168.1.1 - - [10/Oct/2024:13:55:36 +0000] "GET /index.html HTTP/1.1" 200 2326 "Mozilla/5.0"
10.0.0.5 - - [10/Oct/2024:13:56:01 +0000] "POST /api/data HTTP/1.1" 201 512 "curl/7.88.1"
```

Нужно получить:
1. Топ-10 IP-адресов по числу запросов;
2. Распределение HTTP status codes (200, 404, 500 и т.д.).

### 4.2 Парсинг логов

Первый шаг — распарсить сырую строку лога. В Hadoop (практика 1.6) это делалось в Mapper'е. В Spark мы делаем то же самое, но на Python.

Создайте файл `spark_weblog_analysis.py`:

```python
import re
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract, count as spark_count


def parse_log_line(line: str) -> tuple:
    """
    Парсит одну строку веб-лога.
    Пример входных данных:
      192.168.1.1 - - [10/Oct/2024:13:55:36 +0000] "GET /index.html HTTP/1.1" 200 2326 "Mozilla/5.0"
    
    Возвращает: (ip, method, path, status, size)
    """
    pattern = r'^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"(\S+)\s+(\S+)\s+\S+"\s+(\d+)\s+(\d+)'
    match = re.match(pattern, line)
    if match:
        return (match.group(1), match.group(3), match.group(4),
                int(match.group(5)), int(match.group(6)))
    return None


def main():
    # SparkSession создаёт SparkContext автоматически
    spark = SparkSession.builder \
        .appName("WebLog-Analysis") \
        .master("local[*]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    sc = spark.sparkContext

    input_path = "datasets/web_logs.txt"

    # ========================================
    # Вариант 1: RDD API
    # ========================================
    print("\n--- RDD API ---")

    # Загружаем файл как RDD строк
    rdd = sc.textFile(input_path)

    # Парсим каждую строку, отбрасываем невалидные
    parsed_rdd = rdd.map(parse_log_line).filter(lambda x: x is not None)

    # Топ-10 IP: map → (ip, 1) → reduceByKey → top(10)
    ip_counts = (parsed_rdd
        .map(lambda x: (x[0], 1))          # (ip, 1)
        .reduceByKey(lambda a, b: a + b)   # суммируем по ip
        .top(10, key=lambda x: x[1])       # берём 10 самых частых
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
    print("\n--- DataFrame API ---")

    df = spark.read.text(input_path)

    # Извлекаем поля из строки с помощью регулярных выражений
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
    print(f"\nTotal requests: {log_df.count()}")
    print(f"Unique IPs: {log_df.select('ip').distinct().count()}")
    avg_size = log_df.agg({"size": "avg"}).collect()[0][0]
    print(f"Average response size: {avg_size:.0f} bytes")

    sc.stop()
    spark.stop()


if __name__ == "__main__":
    main()
```

**Ключевые моменты:**

1. `parse_log_line()` — аналог Mapper'а из Hadoop. Регулярное выражение извлекает IP, метод, путь, статус и размер из каждой строки лога.
2. `filter(lambda x: x is not None)` — отбрасывает строки, которые не удалось распарсить (аналог `try/catch` в Mapper'е).
3. `top(10, key=...)` — аналог сортировки и `LIMIT 10` в SQL.
4. В DataFrame API мы используем `regexp_extract()` — Spark сам применит оптимизации при выполнении.

Запуск:

```bash
python spark_weblog_analysis.py
```

---

## 5. Сравнение: Hadoop vs Spark

### 5.1 Что изменилось

Вспомните реализацию WordCount в Hadoop (практика 1.6):

```java
// Hadoop MapReduce — Java, 60+ строк
public static class WordCountMapper extends Mapper<...> {
    @Override
    protected void map(Object key, Text value, Context context) {
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }
}
// + Reducer, + Job configuration, + компиляция, + jar
```

Тот же код на PySpark (RDD API):

```python
# PySpark — Python, 5 строк
word_counts = (rdd
    .flatMap(lambda line: line.split(" "))
    .map(lambda word: (word.lower(), 1))
    .reduceByKey(lambda a, b: a + b)
)
```

**Разница очевидна:** Spark требует в 10-15 раз меньше кода, не требует компиляции и упаковки в JAR.

### 5.2 Производительность

| Метрика | Hadoop MapReduce | Apache Spark |
|---------|------------------|--------------|
| Запись на диск | После каждого этапа | Только при необходимости |
| Время WordCount (1 ГБ) | ~5 мин | ~30 сек |
| Итеративные алгоритмы | Новый Job на каждый проход | Кэширование в памяти |
| Язык | Java (версия 8) | Python, Scala, Java, R |

### 5.3 Когда использовать что

- **Hadoop MapReduce** — если данные не помещаются в память, или нужна максимальная надёжность (данные всегда на диске с репликацией).
- **Spark** — в 90% случаев. Быстрее, проще, поддерживает SQL, потоковую обработку и машинное обучение.

> **Важно:** Spark не заменяет Hadoop полностью. HDFS остаётся надёжной файловой системой, а YARN — менеджером ресурсов. Spark может работать поверх YARN, используя HDFS для хранения данных.

---

## 6. Самостоятельные задания

### Задание 1. Top-K самых частых слов (3 балла)

**Зачем это нужно:** В реальной аналитике редко нужны все слова — обычно интересуются только самыми частыми. Например, для поиска трендов или ключевых слов.

**Что сделать:**

1. Возьмите за основу код WordCount из раздела 3.
2. Добавьте возможность задавать K через командную строку (используйте `argparse`):

```bash
python spark_topk.py --k 5
```

3. Реализуйте оба варианта: RDD API и DataFrame API.

**Подсказка:**

- RDD: используйте `.top(K, key=lambda x: x[1])` — эта функция уже сортирует и берёт первые K элементов;
- DataFrame: используйте `.orderBy(col("count").desc()).limit(K)`.

**Критерий выполнения:** программа принимает параметр K, выводит результат, код в репозитории.

---

### Задание 2. Расширенный анализ веб-логов (4 балла)

**Зачем это нужно:** Простого подсчёта IP и статус-кодов недостаточно для полноценного анализа. В реальной работе нужно понимать: какие страницы популярны, сколько ошибок, кто чаще всего ошибается.

**Что сделать:**

Расширьте программу из раздела 4, добавив 5 дополнительных метрик:

1. **Топ-10 запрашиваемых путей** — какие страницы самые популярные;
2. **Распределение HTTP методов** — сколько GET, POST, PUT, DELETE;
3. **Анализ ошибок** — сколько запросов вернуло 4xx и 5xx;
4. **Топ-10 IP с наибольшим количеством ошибок** — кто чаще всего получает 403/404;
5. **Средний размер ответа** для успешных запросов (status 200).

**Подсказка:** используйте `.filter(col("status") >= 400)` для фильтрации ошибок и `.agg({"size": "avg"})` для среднего размера.

**Критерий выполнения:** программа выводит все 5 метрик, код реализован на DataFrame API.

---

### Задание 3. Кэширование — почему Spark быстрее (4 балла)

**Зачем это нужно:** Главная причина, почему Spark быстрее Hadoop — кэширование в памяти. Если один и тот же набор данных используется несколько раз (например, в итеративных алгоритмах ML), Spark не читает его с диска каждый раз, а хранит в памяти.

**Что сделать:**

1. Загрузите `datasets/web_logs.txt`.
2. Выполните одинаковую цепочку трансформаций дважды:
   - **Вариант A:** без кэширования — выполните действие 3 раза подряд и замерьте время каждого;
   - **Вариант B:** с кэшированием — вызовите `.cache()` перед вычислениями, выполните действие 3 раза.
3. Сравните результаты.

**Ожидаемый результат:**

```
Without cache:
  Iteration 1: 0.45s
  Iteration 2: 0.42s
  Iteration 3: 0.44s
  Average: 0.44s

With cache:
  Iteration 1: 0.43s  ← первое вычисление + кэширование
  Iteration 2: 0.05s  ← данные уже в памяти!
  Iteration 3: 0.04s  ← данные уже в памяти!
  Average: 0.17s
```

**Критерий выполнения:** реализовано сравнение, показан эффект кэширования, в отчёте объяснение, почему так произошло.

