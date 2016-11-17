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
		println(s"Default Partition Count:${sc.defaultMinPartitions}")
		val data = DataRetriever.retrieveTweets(args(0),sc)
		val cleaned = CleanTweet.clean(data,sc)
		val predicted = Word2VecClassifier.predict(cleaned,sc)
		DataWriter.writeTweets(predicted)
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
