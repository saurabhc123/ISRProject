package isr.project

import org.apache.spark.mllib.linalg.Word2VecClassifier
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
object SparkGrep {
  def tweetchange(tweet: Tweet): Tweet = {
    if (tweet.label.get == 0.0) {
      return Tweet(tweet.id, tweet.tweetText, Option(9.0))
    }
    return Tweet(tweet.id, tweet.tweetText, tweet.label)
  }

  def main(args: Array[String]) {
    if (args.length == 2){
      val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("HBaseProductExperiments")
      val sc = new SparkContext(conf)
      HBaseExperiment(args(0),args(1),sc)
      System.exit(0)
    }
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
  def getTweetsFromFile(fileName:String,labelMap:Map[String,Double], sc: SparkContext): Array[Tweet] = {
    val file = sc.textFile(fileName)
    val allProductNum = file.map(x => x.split("; ")).filter(!_.isEmpty).map(x => x(0)).distinct().collect()
    var maxLab = 0.0
    if (labelMap.nonEmpty ){
      maxLab = labelMap.valuesIterator.max + 1
    }
    for (num <- allProductNum){
      if (!labelMap.contains(num)){
        labelMap + (num -> maxLab)
        maxLab = maxLab + 1
      }
    }
    file.map(x => x.split("; ")).filter(!_.isEmpty).map(x => Tweet(x(1),x(2), labelMap.get(x(0)))).collect()
  }
  def HBaseExperiment(trainFile:String, testFile:String,sc: SparkContext): Unit ={
    var labelMap = Map[String,Double]()
    val training_partitions = 8
    val testing_partitions = 8
    val trainTweets = getTweetsFromFile(trainFile,labelMap,sc)
    val testTweets = getTweetsFromFile(testFile,labelMap,sc)
    Word2VecClassifier._lrModelFilename = trainFile +"lrModel"
    Word2VecClassifier._word2VecModelFilename = trainFile +"w2vModel"
    Word2VecClassifier._numberOfClasses = trainTweets.map(x => x.label).distinct.length
    val trainTweetsRDD = sc.parallelize(trainTweets,training_partitions)
    val cleaned_trainingTweetsRDD = sc.parallelize(CleanTweet.clean(trainTweetsRDD,sc).collect(),training_partitions)
    // start timer?
    Word2VecClassifier.train(cleaned_trainingTweetsRDD,sc)

  }
}