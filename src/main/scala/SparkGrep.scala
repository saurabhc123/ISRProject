package isr.project

import org.apache.spark.mllib.linalg.Word2VecClassifier
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
object SparkGrep {
  def tweetchange(tweet: Tweet): Tweet = {
    if (tweet.label.get == 0.0) {
      return Tweet(tweet.id, tweet.tweetText, Option(9.0))
    }
    return Tweet(tweet.id, tweet.tweetText, tweet.label)
  }

  def main(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkGrep <collection number to process> <number of classes> <blockCount>")
      System.exit(1)
    }
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val start = System.currentTimeMillis()
    //Word2VecClassifier.run(args, '|')
    val sc = new SparkContext()
    val readTweets = DataRetriever.retrieveTweets(args, sc)
    //val cleanTweets = CleanTweet.clean(readTweets,sc)
    //val predictedTweets = Word2VecClassifier.predict(cleanTweets,sc)
    //DataWriter.writeTweets(predictedTweets)
    //MultiClassOrchestrator.train(args, '|')
    //Orchestrator.train(args)
    //FpGenerate.generateFrequentPatterns("data/issac.txt", args)
    //SparkUtilities.countWords(args)
    //WordVectorGenerator.generateWordVector("data/issac.txt", args)
    //CleanTweet.clean(args,"data/multi_class_lem")
    val end = System.currentTimeMillis()
    println(s"Took ${(end - start) / 1000.0} seconds for the whole process.")
  }
}