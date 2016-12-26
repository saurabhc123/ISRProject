import isr.project.{SparkGrep, Tweet}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Word2VecClassifier
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Eric on 12/23/2016.
  */
object ExperimentRunner {

  val training_partitions = 8
  val testing_partitions = 8

  def main(args: Array[String]): Unit = {
    // for the product datasets
    /*val base_dirs = Seq(
      "data/product_data/uol-electronic",
      "data/product_data/uol-non-electronic",
      "data/product_data/uol-book"
    )
    //val suffix = (0 to 9).toList.map(_.toString)
    */
    // for the small tweet datasets
    val base_dirs = Seq("data/accuracy_experiment/data")
    val suffix = (2 to 10).toList.map(_.toString + ".rw")

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ExperimentOrchestration")
    val sc = new SparkContext(conf)
    turnOffLogging(sc)
    var allMetrics = List[List[List[ExperimentalMetrics]]]()
    for (dir <- base_dirs) {
      // for the product datasets
      //val experimentalMetrics = run_experiments(dir,"/train","/test",suffix,3,sc)
      // for the tweet datasets
      val experimentalMetrics = run_experiments(dir, "/train_data", "/test_data", suffix, 3, sc)
      allMetrics = allMetrics :+ experimentalMetrics
    }
    for ((dir, idx) <- base_dirs.zipWithIndex) {
      println(s"####Experiment dir $dir####")
      val experimentMetrics = allMetrics(idx)
      println(s"###Per set ###")
      for (setMetric <- experimentMetrics) {
        println(ExperimentalMetrics.header())
        for (met <- setMetric) {
          println(met.toString())
        }
      }
      println(s"## ALL ##")
      println(ExperimentalMetrics.header())
      for (metric <- experimentMetrics.flatten) {
        println(metric.toString())
      }

    }
  }

  def run_experiments(dir: String,trainFile : String, testFile:String , suffixes: List[String], timesToRunEach: Int, sc:SparkContext): List[List[ExperimentalMetrics]] = {
      var experimentSet = List[List[ExperimentalMetrics]]()
      for (suffix <- suffixes){
        val trainFName = dir + trainFile + suffix
        val testFName = dir + testFile + suffix
        var oneSet = List[ExperimentalMetrics]()
        for (i <- 1 to timesToRunEach){
          println(s"Running ${trainFName} ${testFName}")
          val m = scala.collection.mutable.Map[String,Double]()
          val trainTweets = SparkGrep.getTweetsFromFile(trainFName,m,sc)
          val trainTweetsRDD = sc.parallelize(trainTweets,training_partitions)
          val testTweets = SparkGrep.getTweetsFromFile(testFName,m,sc)
          val testTweetsRDD = sc.parallelize(testTweets,testing_partitions)
          SparkGrep.SetupWord2VecField(trainFName,trainTweets)
          val (word2VecModel, logisticRegressionModel,trainTime) = SparkGrep.PerformTraining(sc, trainTweetsRDD)
          val (predictionTweets,predictionLabel) = performPrediction(sc,word2VecModel,logisticRegressionModel,testTweetsRDD)

          //This is how the IDF based classifier would run.
          val (idfModel, hashingTfModel, idfLrModel, idfTrainTime) = SparkGrep.PerformIDFTraining(sc, trainTweetsRDD)
          val (predictionIDFTweets, predictionIDFLabels) = Word2VecClassifier.predictForIDFClassifer(testTweetsRDD, sc, idfModel, hashingTfModel, idfLrModel)

          val Metrics = MetricsCalculator.GenerateClassifierMetrics(predictionLabel)
          Metrics.trainTime = trainTime
          oneSet = oneSet :+ Metrics
        }
        experimentSet = experimentSet :+ oneSet

      }
    return experimentSet
  }

  def performPrediction(sc: SparkContext, word2VecModel: Word2VecModel, logisticRegressionModel: LogisticRegressionModel, testTweetRdd: RDD[Tweet]): (RDD[Tweet], RDD[(Double, Double)]) = {
    val (predictionTweets, predictionLabel) = Word2VecClassifier.predict(testTweetRdd, sc, word2VecModel, logisticRegressionModel)
    (predictionTweets, predictionLabel)
  }

  private def turnOffLogging(sc: SparkContext) = {
    sc.setLogLevel(Level.ERROR.toString)
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getRootLogger.setLevel(Level.ERROR)
  }
}
