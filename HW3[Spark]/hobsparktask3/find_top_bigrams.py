from pyspark.sql import SparkSession
import pyspark.sql.types as PySparkTypes
from pyspark.sql.window import Window
import pyspark.sql.functions as f
import re


def cut_words(item):
    articleID, text = item[0], item[1].split()
    return [[articleID, re.sub("\W+", "", word.lower()), pos] for pos, word in enumerate(text)]


def read_and_prepare_dataset():
    articles_schema = PySparkTypes.StructType(fields=[
        PySparkTypes.StructField('id', PySparkTypes.IntegerType(), False),
        PySparkTypes.StructField('text', PySparkTypes.StringType(), False)
    ])

    stop_words_schema = PySparkTypes.StructType(fields=[
        PySparkTypes.StructField('word', PySparkTypes.StringType(), False)
    ])

    articles = spark.read.csv('/data/wiki/en_articles_part', sep='\t', schema=articles_schema)

    stop_words = spark.read.csv('/data/wiki/stop_words_en-xpo6.txt', stop_words_schema)  # there just 300 words
    stop_words = f.broadcast(stop_words)  # broadcast small df to make every join faster

    articles = articles.repartition(NUMBER_OF_PARTITIONS)

    words = articles.rdd.flatMap(cut_words).toDF(['articleId', 'word', 'pos'])
    # words = words.withColumn("word", f.regexp_replace(f.col("word"), "[^a-z0-9]", ""))
    words = words.withColumn('word', f.trim(f.col('word'))).filter(words['word'].isNotNull())

    # words.word = f.array_except(words.word, stop_words.word)
    words = words.join(stop_words, 'word', 'left_anti')

    return words


NUMBER_OF_PARTITIONS = 13
MINIMAL_BIGRAMS_APPEARANSE = 500
TOP_VALUES_NUMBER = 38

spark = SparkSession.builder.config('spark.sql.shuffle.partitions', NUMBER_OF_PARTITIONS)\
    .appName('alekseyen').master('yarn').getOrCreate()


words = read_and_prepare_dataset()

words_all_count = words.count()
words_proba = words.groupBy('word').count().withColumn('proba', f.col('count') / words_all_count)

bigrams = words.withColumn('first_word_in_pair',
                           f.lag(words['word']).over(Window.partitionBy('articleId').orderBy('pos'))) \
                .withColumnRenamed('word', 'second_word_in_pair')

bigrams_count = bigrams.count()
bigrams_proba = bigrams.groupBy('first_word_in_pair', 'second_word_in_pair').count() \
    .filter(f.col('count') >= MINIMAL_BIGRAMS_APPEARANSE) \
    .withColumn('joint_proba', f.col('count') / bigrams_count)

bigrams_full_proba = bigrams_proba.join(words_proba, bigrams_proba['first_word_in_pair'] == words_proba['word'], 'inner') \
    .withColumnRenamed('proba', 'first_proba') \
    .drop('word') \
    .join(words_proba, bigrams_proba['second_word_in_pair'] == words_proba['word'], 'inner') \
    .withColumnRenamed('proba', 'second_proba')

bigrams_npmi = bigrams_full_proba.withColumn('pmi',
                                            f.log(f.col('joint_proba') / (f.col('first_proba') * f.col('second_proba')))) \
                                .withColumn('npmi', -f.col('pmi') / f.log(f.col('joint_proba')))

bigrams_top = bigrams_npmi.orderBy('npmi', ascending=False) \
    .select(f.concat_ws('_', f.col('first_word_in_pair'), f.col('second_word_in_pair')).alias('bigram')) \
    .take(TOP_VALUES_NUMBER)

for line in bigrams_top:
    print(line.bigram)
