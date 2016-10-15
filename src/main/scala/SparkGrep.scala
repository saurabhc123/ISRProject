package scala

object SparkGrep {
	def main(args: Array[String]) {

		if (args.length < 3) {
			System.err.println("Usage: SparkGrep <host> <input_file> <match_term>")
			System.exit(1)
		}

		Orchestrator.train(args)
    //Orchestrator.train(args)
		//FpGenerate.generateFrequentPatterns("data/issac.txt", args)
		//SparkUtilities.countWords(args)
		//WordVectorGenerator.generateWordVector("data/issac.txt", args)
		//CleanTweet.clean(args,"data/multi_class_lem")
	}

}
