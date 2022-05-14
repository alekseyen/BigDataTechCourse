import time
from hyperloglog import HyperLogLog
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import subprocess
from hdfs import Config
import os

BATCH_TIMEOUT = 2


def get_device_type(logs):
    user_id, device_info = logs.split('\t')
    res = []
    if device_info.find('iPhone') != -1:
        res.append(('seg_iphone', user_id))
    if device_info.find('Firefox') != -1:
        res.append(('seg_firefox', user_id))
    if device_info.find('Windows') != -1:
        res.append(("seg_windows", user_id))

    return res


def update_count(values, old):
    old = old or HyperLogLog(0.01)

    for v in values:
        old.add(v)
    return old


def set_ending_flag(rdd):
    global finished
    if rdd.isEmpty():
        finished = True


def print_rdd(rdd):
    global top_devices

    if finished:
        top_devices = rdd.take(10)


client = Config('../hdfscli.cfg').get_client()
nn_address = subprocess.check_output('hdfs getconf -confKey dfs.namenode.http-address', shell=True).strip().decode(
    "utf-8")
sc = SparkContext(master='yarn-client')
ssc = StreamingContext(sc, BATCH_TIMEOUT)
ssc.checkpoint('./checkpoint{}'.format(time.strftime("%Y_%m_%d_%H_%M_%s", time.gmtime())))

# create batches from hdfs file
DATA_PATH = "/data/course4/uid_ua_100k_splitted_by_5k"
batches = [sc.textFile(os.path.join(*[nn_address, DATA_PATH, path])) for path in client.list(DATA_PATH)]
# every BATCH_TIMEOUT seconds send batches
dstream = ssc.queueStream(rdds=batches)

top_devices = None
finished = False

dstream.foreachRDD(set_ending_flag)

dstream \
    .flatMap(get_device_type) \
    .updateStateByKey(update_count) \
    .map(lambda type: (type[0], len(type[1]))) \
    .foreachRDD(lambda rdd: print_rdd(rdd.sortBy(lambda x: -x[1])))

ssc.start()
while not finished:
    pass
ssc.stop()
sc.stop()

for row in top_devices:
    print('{}\t{}'.format(*row))
