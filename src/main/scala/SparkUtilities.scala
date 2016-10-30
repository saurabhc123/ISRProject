package isr.project


import org.apache.spark.{SparkConf, SparkContext}


/**
 * Created by saur6410 on 9/27/16.
 */
object SparkUtilities {

  def countWords(args: Array[String]): Unit = {
		val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
		val sc = new SparkContext(conf)
		val inputFile = sc.textFile(args(1), 2).cache()
		val matchTerm: String = args(2)
		val numMatches = inputFile.filter(line => line.contains(matchTerm)).count()
		println("%s lines in %s contain %s".format(numMatches, args(1), matchTerm))
		System.exit(0)
	}


//	def trainClassifier(trainingFilename: String): LogisticRegressionWithLBFGS ={
//    val p = new LabeledPoint(0.0,Vectors.dense(0.245))
//
//	}


//	def createNLPPipeline(): StanfordCoreNLP = {
//		val props = new Properties()
//		props.put("annotators", "tokenize, ssplit, pos, lemma")
//		new StanfordCoreNLP(props)
//	}
//
//def plainTextToLemmas(text: String, stopWords: Set[String],    pipeline: StanfordCoreNLP): Seq[String] =
//	{
//		val doc = new Annotation(text)
//		pipeline.annotate(doc)
//		val lemmas = new ArrayBuffer[String]()
//		val sentences = doc.get(classOf[SentencesAnnotation])
//		for (sentence <- sentences;token <- sentence.get(classOf[TokensAnnotation]))
//			{
//				val lemma = token.get(classOf[LemmaAnnotation])
//				if (lemma.length > 2 && !stopWords.contains(lemma)  && isOnlyLetters(lemma))
//				{
//					lemmas += lemma.toLowerCase
//				}
//			}
//		lemmas
//	}
//
//		def isOnlyLetters(str: String): Boolean = {
//		str.forall(c => Character.isLetter(c))
//	}

	//val stopWords = sc.broadcast(  scala.io.Source.fromFile("stopwords.txt).getLines().toSet).valueval lemmatized: RDD[Seq[String]] = plainText.mapPartitions(it => {  val pipeline = createNLPPipeline()  it.map { case(title, contents) =>    plainTextToLemmas(contents, stopWords, pipeline)  }})


}
