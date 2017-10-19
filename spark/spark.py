# -*- coding: utf-8 -*-
"""
Created on Thu Sep 28 15:19:33 2017

@author: shanlin
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
from pyspark.sql import HiveContext, Row
import json

sc = SparkContext("local[*]", "mysqltohive")
ssc = StreamingContext(sc,1)
hsql = HiveContext(sc)

topic = "test"
brokers = "datanode1:9092,datanode3:9092,datanode6:9092"
parttiton = 0
start = 8390
topicpartion = TopicAndPartition(topic, parttiton)
fromoffset = {topicpartion: long(start)}
dkafka = KafkaUtils.createDirectStream(ssc,[topic], \
         {"metadata.broker.list": brokers},fromOffsets = fromoffset)

offsetRanges = []

def storeOffsetRanges(rdd):
     global offsetRanges
     offsetRanges = rdd.offsetRanges()
     return rdd

def printOffsetRanges(rdd):
     for o in offsetRanges:
         print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

dkafka.transform(storeOffsetRanges).foreachRDD(printOffsetRanges)



#+---+----+---+-----------+
#| id|name|age|create_time|
#+---+----+---+-----------+
#+---+----+---+-----------+

     
def func1(x):
#	if  not x.isEmpty():
            dictx = json.loads(json.loads(json.dumps(x[1])))
            dbname = dictx["database"]
            tablename = dictx["table"]
            types = dictx["type"]
            data = dictx["data"]
            datavs = [data["id"],data["name"], data["age"], data["create_time"]]
            return datavs


def func2(x):
    if not x.isEmpty():
        xdf = hsql.createDataFrame(x,["id", "name", "age", "create_time"])
        hsql.registerDataFrameAsTable(xdf, "temptable1")
        hsql.sql("insert into dev.test select id, name, age, create_time from temptable1")


dd = dkafka.map(lambda x: func1(x))
dd.foreachRDD(func2)


ssc.start()
ssc.awaitTermination()
