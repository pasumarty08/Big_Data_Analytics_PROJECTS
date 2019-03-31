from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
    # print(pwords)
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    This function plots the counts of positive and negative words for each timestep.
    """
    positiveCounts = []
    negativeCounts = []
    time = list(range(len(counts)))
    
    for i in counts:
        positiveCounts.append(i[1][1])
        negativeCounts.append(i[0][1])
    
    # Green for Positive
    posLine = plt.plot(time, positiveCounts,'go-', label='Positive')
    # Red for Negative
    negLine = plt.plot(time, negativeCounts,'ro-', label='Negative')
    plt.axis([0, len(counts)+2, 0, max(max(positiveCounts)+10000, max(negativeCounts))+50])
    plt.xlabel('Time step')
    plt.ylabel('Word count')
    plt.legend(loc = 'upper left')
    plt.show()
    
    
def load_wordlist(filename):
    #This function should return a list or set of words from the given filename.
    # YOUR CODE HERE
    
    wordDict = {}
    # https://docs.python.org/2.3/whatsnew/node7.html
    temp_file = open(filename, 'rU')
    f = temp_file.read()
    f = f.split('\n')
    for line in f:
        wordDict[line] = 1
    temp_file.close()
    return wordDict  

## https://spark.apache.org/docs/latest/streaming-programming-guide.html
def updateFunction(newValues, runningCount):
    if runningCount is None:
        runningCount = 0
    return sum(newValues, runningCount) 

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1])

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE
    
    ## https://spark.apache.org/docs/2.2.0/streaming-programming-guide.html
    words = tweets.flatMap(lambda line: line.split(" "))
  
    #posWords = words.map(lambda x: ("positive", 1) if x in pwords elif x in nwords ("positive", 0))
    posWords = words.map(lambda x: ("positive", 1) if x in pwords.keys() else ("positive", 0))
    negWords = words.map(lambda x: ("negative", 1) if x in nwords.keys() else ("negative", 0))
    
    # Transformations that can join two TransformedDStreams
    # http://sungsoo.github.io/2015/04/06/transformations-on-dstreams.html
    """
    allWords = posWords.join(negWords)
    print("----------------------------------------------")
    print(allWords.pprint())
    print("----------------------------------------------")
    """
    # The above doesn't work because we dont have the same key values. 
    # It gets stuck during execution
    
    allWords = posWords.union(negWords)
    combWords = allWords.reduceByKey(lambda x,y: x+y)
    # Post @298 by Ronil
    runningSentimentCounts = combWords.updateStateByKey(updateFunction)
    runningSentimentCounts.pprint()
    
    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    # YOURDSTREAMOBJECT.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    combWords.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)
    return counts


if __name__=="__main__":
    main()
