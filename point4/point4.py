
import sys
import json
from pyspark import SparkContext, SparkConf


def getJsonValue(jsonline):
        JSON = json.loads(jsonline)
        return JSON["entities"]["hashtags"]




if __name__ == "__main__":

        sc = SparkContext("local","PySpark Word Count Exmaple")
        file = sc.textFile("/home/ubuntu/data/trump_tweets.txt")        
        hashtags = file.flatMap(lambda line: getJsonValue(line))
        words = hashtags.map(lambda hashtag: hashtag["text"])
        wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
        #wordCounts.sortBy(lambda (word, count): count)
        wordCounts.saveAsTextFile("/home/ubuntu/spark/taller/point4/output")
