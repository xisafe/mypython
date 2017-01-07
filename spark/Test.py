import sys
import os
from operator import add
os.environ['SPARK_HOME'] = "D:/spark-2.0.1-bin-hadoop2.6"
sys.path.append("D:/spark-2.0.1-bin-hadoop2.6/python")
sys.path.append("D:/spark-2.0.1-bin-hadoop2.6/python/build")
try:
    from pyspark import SparkContext,SparkConf
    print("Successfully imported Spark Modules")
except ImportError as e:
    print("Can not import Spark Modules", e)
    sys.exit(1)