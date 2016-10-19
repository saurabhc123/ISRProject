

package org.apache.spark.mllib.linalg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

case class Tweet(id: String, tweetText: String, label: Option[Double] = None)

object Word2VecClassifier{
  var _numOfClasses = 2

  def run(args: Array[String]) {

    if (args.length < 3) {
      System.err.println("Usage: SparkGrep <host> <input_file> <numberofClasses>")
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val inputFilename = args(1)
    _numOfClasses = args(2).toInt

    def printRDD(xs: RDD[_]) {
      println("--------------------------")
      xs take 5 foreach println
      println("--------------------------")
    }

    val delimiter = ";"
    val conf = new SparkConf(false).setMaster("local").setAppName("Word2Vec")
    val sc = new SparkContext(conf)

    // Load
    val trainPath = inputFilename
    val testPath = inputFilename

    // Load text
    def skipHeaders(idx: Int, iter: Iterator[String]) = if (idx == 0) iter.drop(1) else iter

    val trainFile = sc.textFile(trainPath) mapPartitionsWithIndex skipHeaders map (l => l.split(delimiter))
    val testFile = sc.textFile(testPath) mapPartitionsWithIndex skipHeaders map (l => l.split(delimiter))

    // To sample
    def toTweet(segments: Array[String]) = segments match {
      case Array(label, tweetText) => Tweet(java.util.UUID.randomUUID.toString, tweetText, Some(label.toDouble))
    }

    val trainingTweets = trainFile map toTweet
    val testTweets = testFile map toTweet

    // Clean Html
    def cleanHtml(str: String) = str.replaceAll( """<(?!\/?a(?=>|\s.*>))\/?.*?>""", "")

    def cleanTweetHtml(sample: Tweet) = sample copy (tweetText = cleanHtml(sample.tweetText))

    val cleanTrainingTweets = trainingTweets map cleanTweetHtml
    val cleanTestTweets = testTweets map cleanTweetHtml

    // Words only
    def cleanWord(str: String) = str.split(" ").map(_.trim.toLowerCase).filter(_.size > 0).map(_.replaceAll("\\W", "")).reduce((x, y) => s"$x $y")

    def wordOnlySample(sample: Tweet) = sample copy (tweetText = cleanWord(sample.tweetText))

    val wordOnlyTrainSample = cleanTrainingTweets map wordOnlySample
    val wordOnlyTestSample = cleanTestTweets map wordOnlySample

    // Word2Vec
    val samplePairs = wordOnlyTrainSample.map(s => s.id -> s).cache()
    val reviewWordsPairs: RDD[(String, Iterable[String])] = samplePairs.mapValues(_.tweetText.split(" ").toIterable)
    println("Start Training Word2Vec --->")
    val word2vecModel = new Word2Vec().fit(reviewWordsPairs.values)

    println("Finished Training")
    println(word2vecModel.transform("hurricane"))
    println(word2vecModel.findSynonyms("shooting", 4))

    def wordFeatures(words: Iterable[String]): Iterable[Vector] = words.map(w => Try(word2vecModel.transform(w))).filter(_.isSuccess).map(x => x.get)

    def avgWordFeatures(wordFeatures: Iterable[Vector]): Vector = Vectors.fromBreeze(wordFeatures.map(_.asBreeze).reduceLeft((x, y) => x + y) / wordFeatures.size.toDouble)

    def filterNullFeatures(wordFeatures: Iterable[Vector]): Iterable[Vector] = if (wordFeatures.isEmpty) wordFeatures.drop(1) else wordFeatures

    // Create feature vectors
    val wordFeaturePair = reviewWordsPairs mapValues wordFeatures
    val intermediateVectors = wordFeaturePair.mapValues(x => x.map(_.asBreeze))
    val inter2 = wordFeaturePair.filter(!_._2.isEmpty)
    val avgWordFeaturesPair = inter2 mapValues avgWordFeatures
    val featuresPair = avgWordFeaturesPair join samplePairs mapValues {
      case (features, Tweet(id, tweetText, label)) => LabeledPoint(label.get, features)
    }
    val trainingSet = featuresPair.values

    // Classification
    println("String Learning and evaluating models")
    val Array(x_train, x_test) = trainingSet.randomSplit(Array(0.7, 0.3))
    // Run training algorithm to build the model
    val logisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(_numOfClasses)
      .run(x_train)

    val trainingRDD = x_train.toJavaRDD()
    //val svmModel = SVMMultiClassOVAWithSGD.train(trainingRDD, 100 )
    // Compute raw scores on the test set.
    val logisticRegressionPredictions = x_test.map { case LabeledPoint(label, features) =>
      val prediction = logisticRegressionModel.predict(features)
      (prediction, label)
    }

    GenerateClassifierMetrics(logisticRegressionPredictions, "Logistic Regression")

    println("<---- done")
    Thread.sleep(10000)
  }

  def GenerateClassifierMetrics(predictionAndLabels: RDD[(Double, Double)],classifierType : String): Unit = {
    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)

    for (i <- 0 to _numOfClasses - 1) {
      val classLabel = i
      println(s"\n***********   Class:$classLabel   *************")
      println(s"F1 Score:${metrics.fMeasure(classLabel)}")
      println(s"True Positive:${metrics.truePositiveRate(classLabel)}")
      println(s"False Positive:${metrics.falsePositiveRate(classLabel)}")
    }

    println(s"\nConfusion Matrix \n${metrics.confusionMatrix}")

    val accuracy = metrics.accuracy
    val precision = metrics.weightedPrecision
    val recall = metrics.weightedRecall
    println(s"\n***********   Classifier Results for $classifierType   *************")
    println(s"Accuracy = $accuracy")
    println(s"Weighted Precision = $precision")
    println(s"Weighted Recall = $recall")

    println(s"\n***********   End of Classifier Results for $classifierType   *************")
  }
 }



