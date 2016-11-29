package isr.project

import org.apache.spark.mllib.linalg.Word2VecClassifier
import org.apache.spark.SparkContext
object SparkGrep {
	def main(args: Array[String]) {

		if (args.length < 2) {
			System.err.println("Usage: SparkGrep <collection number to process> <number of classes>")
			System.exit(1)
		}
		val start = System.currentTimeMillis()
		//Word2VecClassifier.run(args, '|')
		val sc = new SparkContext()
		val trainingTweets = DataRetriever.getTrainingTweets(sc)
			trainingTweets.collect().foreach(println)
    Word2VecClassifier.train(trainingTweets,sc)
		println(trainingTweets.map(tweet => tweet.label).filter(_.isDefined).map(e => e.get).distinct().collect().sorted.foreach(println))
		//val cleanTweet = CleanTweet.clean(trainingTweets,sc)
		//DataWriter.writeTrainingData(cleanTweet)
    /*val readTweets = DataRetriever.retrieveTweets(args(0),sc)
    val cleanTweets = CleanTweet.clean(readTweets,sc)
    val predictedTweets = Word2VecClassifier.predict(cleanTweets,sc)
		DataWriter.writeTweets(predictedTweets)*/
		//MultiClassOrchestrator.train(args, '|')
    //Orchestrator.train(args)
		//FpGenerate.generateFrequentPatterns("data/issac.txt", args)
		//SparkUtilities.countWords(args)
		//WordVectorGenerator.generateWordVector("data/issac.txt", args)
		//CleanTweet.clean(args,"data/multi_class_lem")
		val end = System.currentTimeMillis()
    println(s"Took ${(end-start)/1000.0} seconds for the whole process.")
	}

}
