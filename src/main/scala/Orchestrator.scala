
package scala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.evaluation.{BinaryClassificationMetrics, MulticlassMetrics}
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, GeneralizedLinearModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by saur6410 on 10/2/16.
 */

object Orchestrator {

  def train(args: Array[String]): Unit = {
    val inputFilename = args(1)
    val conf = new SparkConf().setAppName("SparkGrep").setMaster(args(0))
    val sc = new SparkContext(conf)
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    //Get the training data file passed as an argument
    val trainingFileInput = sc.textFile(inputFilename)
    val fpmPatterns = FpGenerate.generateFrequentPatterns(inputFilename, sc)
    val trainingData = trainingFileInput.map(line => CreateLabeledPointFromInputLine(line, fpmPatterns))

    //Divide the training data into training and test.
    val positiveSamples = trainingData.filter(point => point.label == 1).randomSplit(Array(0.6, 0.4))
    val negativeSamples = trainingData.filter(point => point.label == 0).randomSplit(Array(0.6, 0.4))

    println ("Positive count:"+(positiveSamples(0).count)+"::"+(positiveSamples(1).count))
    println ("Negative count:"+(negativeSamples(0).count)+"::"+(negativeSamples(1).count))
    val trainingPositiveSplit = positiveSamples(0)
    val testPositiveSplit = positiveSamples(1)
    val trainingNegativeSplit = negativeSamples(0)
    val testNegativeSplit = negativeSamples(1)

    val trainingSplit = trainingPositiveSplit ++ trainingNegativeSplit
    val testSplit = testPositiveSplit ++ testNegativeSplit



    val logisticWithBfgs = Classifier.getAlgorithm("logbfgs", 100, Double.NaN, 0.001)
    val svmWithSGD = Classifier.getAlgorithm("svm", 100, 1, 0.001)
    val logisticWithBfgsPredictsActuals=runClassification(logisticWithBfgs, trainingSplit, testSplit)
    val svmWithSGDPredictsActuals=runClassification(svmWithSGD, trainingSplit, testSplit)

    //Test the accuracy of the classifier using the test data
    calculateMetrics(logisticWithBfgsPredictsActuals, "Logistic Regression with BFGS")
    calculateMetrics(svmWithSGDPredictsActuals, "Logistic Regression with SVM")

    //Save the model into a file on HDFS.
  }

  def CreateLabeledPointFromInputLine(line: String, fpmPatterns: RDD[Array[String]]): LabeledPoint = {
    val delimiter = '|'
    val values = line.split(delimiter)
    val label = values(0)
    val documentBody = values(1)
    val fg = new FeatureGenerator(fpmPatterns)
    val features = fg.getFeatures("fpm", documentBody)
    val lp = LabeledPoint(label.toDouble, features)
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
