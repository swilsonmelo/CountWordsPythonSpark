import sys
import json
from pyspark import SparkContext, SparkConf


def getJsonValue(jsonline):
        JSON = json.loads(jsonline)
        return JSON["lang"]


if __name__ == "__main__":

        sc = SparkContext("local","PySpark Word Count Exmaple")
        file = sc.textFile("/home/ubuntu/data/trump_tweets.txt")
        lang = file.map(lambda x: getJsonValue(x) )
        wordCounts = lang.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
        wordCounts.saveAsTextFile("/home/ubuntu/spark/taller/point5/output")


