
package scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.WordVectorGenerator
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by saur6410 on 10/2/16.
 */

object MultiClassOrchestrator {

  var _numOfClasses = 2

  def train(args: Array[String]): Unit = {
    val inputFilename = args(1)
    _numOfClasses = args(2).toInt
    val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    //Get the training data file passed as an argument
    val trainingFileInput = sc.textFile(inputFilename)

    WordVectorGenerator.generateWordVector(inputFilename, sc)
    val data = trainingFileInput.map(line => CreateLabeledPointFromInputLine(line, null))

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)


    // Run training algorithm to build the model
    val logisticRegressionModel = new LogisticRegressionWithLBFGS()
      .setNumClasses(_numOfClasses)
      .run(training)

    val trainingRDD = training.toJavaRDD()
    //val svmModel = SVMMultiClassOVAWithSGD.train(trainingRDD, 100 )
    // Compute raw scores on the test set.
    val logisticRegressionPredictions = test.map { case LabeledPoint(label, features) =>
      val prediction = logisticRegressionModel.predict(features)
      (prediction, label)
    }

    /*val svmPredictions = test.map {x =>
      val lp = LabeledPoint(x.label, x.features)
      val prediction = svmModel.predict(lp)
      (prediction, lp.label)
    }*/

    GenerateClassifierMetrics(logisticRegressionPredictions, "Logistic Regression")
    //GenerateClassifierMetrics(svmPredictions, "SVM with SGD: OVA")
    //Save the model into a file on HDFS.
  }

  def GenerateClassifierMetrics(predictionAndLabels: RDD[(Double, Double)],classifierType : String): Unit = {
    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    val precision = metrics.weightedPrecision
    val recall = metrics.weightedRecall
    println(s"\n***********   Classifier Results for $classifierType   *************")
    println(s"Accuracy = $accuracy")
    println(s"Weighted Precision = $precision")
    println(s"Weighted Recall = $recall")
    for (i <- 0 to _numOfClasses - 1) {
      val classLabel = i
      println(s"\n***********   Class:$classLabel   *************")
      println(s"F1 Score:${metrics.fMeasure(classLabel)}")
      println(s"True Positive:${metrics.truePositiveRate(classLabel)}")
      println(s"False Positive:${metrics.falsePositiveRate(classLabel)}")
    }

    println(s"\nConfusion Matrix \n${metrics.confusionMatrix}")
    println(s"\n***********   End of Classifier Results for $classifierType   *************")
  }

  def CreateLabeledPointFromInputLine(line: String, fpmPatterns: RDD[Array[String]]): LabeledPoint = {
    val delimiter = ';'
    val values = line.split(delimiter)
    val label = values(0)
    //println(s"label: $label")
    val documentBody = values(1)
    val fg = new FeatureGenerator(fpmPatterns)//word2vec
    val features = fg.getFeatures("word2vec", documentBody)
    //val features = fg.getFeatures("fpm", documentBody)
    val lp = LabeledPoint(label.toDouble, features)
    //println(s"$line $lp")
    return lp
  }

  def getModel():Unit = {
    //Load the classifier from the file.

    //return the model
    }

  def predict():Unit = {
    //Take the model and the input vector as the arguments

    //return the prediction
  }


}
