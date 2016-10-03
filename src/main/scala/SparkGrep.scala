package scala

object SparkGrep {
	def main(args: Array[String]) {

		if (args.length < 3) {
			System.err.println("Usage: SparkGrep <host> <input_file> <match_term>")
			System.exit(1)
		}

		Orchestrator.train("data/issac.txt", args)
		//FpGenerate.generateFrequentPatterns("data/issac.txt", args)
		//SparkUtilities.countWords(args)

	}

}
