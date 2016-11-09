package isr.project

import org.apache.spark.mllib.linalg.Word2VecClassifier

object SparkGrep {
	def main(args: Array[String]) {

		if (args.length < 3) {
			System.err.println("Usage: SparkGrep <host> <input_file> <numberofClasses>")
			System.exit(1)
		}
		val start = System.currentTimeMillis()
		Word2VecClassifier.run(args, '|')
		//CleanTweet.clean(DataRetriever.retrieveTweets("45")).foreach(println);
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
