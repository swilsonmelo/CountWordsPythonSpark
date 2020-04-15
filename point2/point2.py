import sys
import json
from pyspark import SparkContext, SparkConf


def getJsonValue(jsonline):
        JSON = json.loads(jsonline)
        return JSON["text"]


if __name__ == "__main__":

        sc = SparkContext("local","PySpark Word Count Exmaple")
        file = sc.textFile("/home/ubuntu/data/trump_tweets.txt")
        text = file.map(lambda x: getJsonValue(x) )
        words = text.flatMap(lambda x: x.split(' '))
        especialWords = words.filter(lambda x: x.lower() not in [ "a", "an", "and", "are", "etc"])
        wordCounts = especialWords.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
        wordCounts.saveAsTextFile("/home/ubuntu/spark/taller/point2/output")


