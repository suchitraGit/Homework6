#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 16:49:39 2019

@author: admin2
"""

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext



if __name__ == "__main__":
    conf = SparkConf().setAppName("Reading files from a directory")
    sc   = SparkContext(conf=conf)
    ssc  = StreamingContext(sc, 5)

    lines = ssc.textFileStream("hdfs://localhost:9000/dir1")
    
    lines.pprint()
    # Split each line into words
    words = lines.flatMap(lambda line: line.split(" "))

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))

    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.pprint()

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate