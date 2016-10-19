

package org.apache.spark.mllib.linalg

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

case class Tweet(id: String, tweetText: String, label: Option[Double] = None)

object Word2VecClassifier extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def printRDD(xs: RDD[_]) {
    println("--------------------------")
    xs take 5 foreach println
    println("--------------------------")
  }

  val delimiter = ";"
  val numberOfClasses = 9
  val conf = new SparkConf(false).setMaster("local").setAppName("Word2Vec")
  val sc = new SparkContext(conf)

  // Load
  val trainPath = s"data/multi_class_lem"
  val testPath = s"data/multi_class_lem"

  // Load text
  def skipHeaders(idx: Int, iter: Iterator[String]) = if (idx == 0) iter.drop(1) else iter

  val trainFile = sc.textFile(trainPath) mapPartitionsWithIndex skipHeaders map (l => l.split(delimiter))
  val testFile = sc.textFile(testPath) mapPartitionsWithIndex skipHeaders map (l => l.split(delimiter))

  // To sample
  def toSample(segments: Array[String]) = segments match {
    //case Array(id, label, tweetText) => Sample(id, tweetText, Some(label.toInt))
    //case Array(id, review) => Sample(id, review)
    case Array(label, tweetText) => Tweet(java.util.UUID.randomUUID.toString , tweetText, Some(label.toDouble))
  }

  val trainSamples = trainFile map toSample
  val testSamples = testFile map toSample

  // Clean Html
  def cleanHtml(str: String) = str.replaceAll( """<(?!\/?a(?=>|\s.*>))\/?.*?>""", "")

  def cleanSampleHtml(sample: Tweet) = sample copy (tweetText = cleanHtml(sample.tweetText))

  val cleanTrainSamples = trainSamples map cleanSampleHtml
  val cleanTestSamples = testSamples map cleanSampleHtml

  // Words only
  def cleanWord(str: String) = str.split(" ").map(_.trim.toLowerCase).filter(_.size > 0).map(_.replaceAll("\\W", "")).reduce((x, y) => s"$x $y")

  def wordOnlySample(sample: Tweet) = sample copy (tweetText = cleanWord(sample.tweetText))

  val wordOnlyTrainSample = cleanTrainSamples map wordOnlySample
  val wordOnlyTestSample = cleanTestSamples map wordOnlySample

  // Word2Vec
  val samplePairs = wordOnlyTrainSample.map(s => s.id -> s).cache()
  val reviewWordsPairs: RDD[(String, Iterable[String])] = samplePairs.mapValues(_.tweetText.split(" ").toIterable)
  println("Start Training Word2Vec --->")
  val word2vecModel = new Word2Vec().fit(reviewWordsPairs.values)

  println("Finished Training")
  println(word2vecModel.transform("hurricane"))
  println(word2vecModel.findSynonyms("shooting", 4))

  def wordFeatures(words: Iterable[String]): Iterable[Vector] = words.map(w => Try(word2vecModel.transform(w))).filter(_.isSuccess).map(x => x.get)

  def avgWordFeatures(wordFeatures: Iterable[Vector]): Vector = Vectors.fromBreeze(wordFeatures.map(_.asBreeze).reduceLeft((x,y) => x + y) / wordFeatures.size.toDouble)

  def filterNullFeatures(wordFeatures: Iterable[Vector]): Iterable[Vector] = if (wordFeatures.isEmpty) wordFeatures.drop(1) else wordFeatures

  // Create feature vectors
  val wordFeaturePair = reviewWordsPairs mapValues wordFeatures
  val intermediateVectors = wordFeaturePair.mapValues(x => x.map(_.asBreeze))
  //val nonNullValues = intermediateVectors.map(x => (!x._2.isEmpty, x) ).filter(_._1).map(v => v)
  val inter2 = wordFeaturePair.filter(!_._2.isEmpty)
  val avgWordFeaturesPair = inter2 mapValues avgWordFeatures
  val featuresPair = avgWordFeaturesPair join samplePairs mapValues {
    case (features, Tweet(id, review, label)) => LabeledPoint(label.get, features)
  }
  val trainingSet = featuresPair.values

  // Classification
  println("String Learning and evaluating models")
  val Array(x_train, x_test) = trainingSet.randomSplit(Array(0.7, 0.3))
  // Run training algorithm to build the model
    val logisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(numberOfClasses)
      .run(x_train)

    val trainingRDD = x_train.toJavaRDD()
    //val svmModel = SVMMultiClassOVAWithSGD.train(trainingRDD, 100 )
    // Compute raw scores on the test set.
    val logisticRegressionPredictions = x_test.map { case LabeledPoint(label, features) =>
      val prediction = logisticRegressionModel.predict(features)
      (prediction, label)
    }



  //val model = SVMWithSGD.train(x_train, 100)

  //val result = model.predict(x_test.map(_.features))

  println(s"10 samples:")
  x_test.map { case LabeledPoint(label, features) => s"$label -> ${logisticRegressionModel.predict(features)}" } foreach println
  val accuracy = x_test.filter(x => x.label == logisticRegressionModel.predict(x.features)).count.toFloat / x_test.count
  println(s"Model Accuracy: $accuracy")

  println("<---- done")
  Thread.sleep(10000)
}

