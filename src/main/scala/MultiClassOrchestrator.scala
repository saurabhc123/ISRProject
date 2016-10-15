
package scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by saur6410 on 10/2/16.
 */

object MultiClassOrchestrator {

  val _numOfClasses = 9

  def train(args: Array[String]): Unit = {
    val inputFilename = args(1)
    val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    //Get the training data file passed as an argument
    val trainingFileInput = sc.textFile(inputFilename)
    //val fpmPatterns = FpGenerate.generateFrequentPatterns(inputFilename, sc)
    WordVectorGenerator.generateWordVector(inputFilename, sc)
    //val trainingData = trainingFileInput.map(line => CreateLabeledPointFromAveragedWordVector(line))
    val data = trainingFileInput.map(line => CreateLabeledPointFromInputLine(line, null))

    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)


    // Run training algorithm to build the model
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(_numOfClasses)
      .run(training)

    // Compute raw scores on the test set.
    val predictionAndLabels = test.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    // Get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val accuracy = metrics.accuracy
    val precision = metrics.weightedPrecision
    val recall = metrics.weightedRecall
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

    //Save the model into a file on HDFS.
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

  def runClassification(algorithm: GeneralizedLinearAlgorithm[_ <: GeneralizedLinearModel], trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint]): RDD[(Double, Double)] = {
    //Train the classifier using the training data
    //Do n-fold cross-validation to choose the best model.
    val model = algorithm.run(trainingData)
    val predicted = model.predict(testData.map(point => point.features))
    val actuals = testData.map(point => point.label)
    val predictsAndActuals: RDD[(Double, Double)] = predicted.zip(actuals)
    predictsAndActuals
  }

  def calculateMetrics(predictsAndActuals: RDD[(Double, Double)], algorithm: String) {
     val accuracy = 1.0*predictsAndActuals.filter(predActs => predActs._1 == predActs._2).count() / predictsAndActuals.count()
     val binMetrics = new BinaryClassificationMetrics(predictsAndActuals)
     println(s"\n************** Printing metrics for $algorithm ***************")
     println(s"Area under ROC ${binMetrics.areaUnderROC}")
     //println(s"Accuracy $accuracy")
     val metrics = new MulticlassMetrics(predictsAndActuals)
     val f1=metrics.fMeasure
     val evalCount = predictsAndActuals.count()
     println(s"F1 $f1")
     println(s"Number of test records: $evalCount")
     println(s"Precision : ${metrics.precision}")

    for (i <- 0 to _numOfClasses - 1) {
      val classLabel = i
      println(s"\nTrue Positive: {$classLabel}\n${metrics.truePositiveRate(classLabel)}")
      println(s"False Positive: {$classLabel}\n${metrics.falsePositiveRate(classLabel)}")
    }

     println(s"Confusion Matrix \n${metrics.confusionMatrix}")
     println(s"************** ending metrics for $algorithm *****************\n")
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
