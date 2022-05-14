import time
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import subprocess
from hdfs import Config
from pyspark.sql import SparkSession
import os

BATCH_TIMEOUT = 10


def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True


client = Config('../hdfscli.cfg').get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode("utf-8")
sc = SparkContext(master='yarn-client')
spark_session = SparkSession.builder \
    .master('yarn') \
    .appName('alekseyen_hw4_2') \
    .getOrCreate()

ssc = StreamingContext(sc, BATCH_TIMEOUT)
ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))

# create batches from hdfs file
DATA_PATH = "/data/graphDFQuarter"
batches = [spark_session.read.parquet(os.path.join(*[nn_address, DATA_PATH, path])).rdd for path in client.list(DATA_PATH)]
# every BATCH_TIMEOUT seconds send batches

dstream = ssc.queueStream(rdds=batches)
finished = False
dstream.foreachRDD(set_ending_flag)


#################### logic starts here

def find_top_common_users(new_values):
    global top_top_common_users
    top_top_common_users = sorted(top_top_common_users + new_values, reverse=True)[:50]


def get_common(text):
    (user_id_1, friends_1), (user_id_2, friends_2) = text
    return len(set(friends_1) & set(friends_2)), user_id_1, user_id_2


top_top_common_users = []

dstream \
    .transform(lambda rdd: rdd.cartesian(rdd)) \
    .map(get_common) \
    .filter(lambda x: x[1] < x[2]) \
    .foreachRDD(lambda rdd: find_top_common_users(rdd.sortBy(lambda x: -x[0]).take(50)))

#################### logic ends here

ssc.start()
while not finished:
    pass

ssc.stop()
sc.stop()

for row in top_top_common_users:
    print('{}\t{}\t{}'.format(*row))
