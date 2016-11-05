package org.apache.spark.ml.linalg

import org.apache.spark.SparkContext
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession



/**
 * Created by saur6410 on 11/4/16.
 */
object NFoldBasedWord2VecClassifier {

  val _numOfClasses = 9;

  def GeneratePredictions(trainingData: RDD[LabeledPoint], testData: RDD[LabeledPoint], sc: SparkContext): (RDD[(Double, Double)], Long) =
  {


    val spark = SparkSession
      .builder()
      .appName(sc.appName)
      .getOrCreate()

    import spark.implicits._

    val lr = new LogisticRegression()
      .setMaxIter(10)
    val ovr = new OneVsRest()
    ovr.setClassifier(lr)



    val trainDF = MLUtils.convertVectorColumnsToML(trainingData.toDF())
    val testDF = MLUtils.convertVectorColumnsToML(testData.toDF())



    // train the multiclass model.
    //val ovrModel = ovr.fit(trainDF)

    val pipeline = new Pipeline()
      .setStages(Array(ovr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val ovrModel = cv.fit(trainDF)

    // score the model on test data.
    val predictions  = ovrModel.transform(testDF)

    // evaluate the model
    val logisticRegressionPredictions = predictions.select("prediction", "label")
      .map(row => (row.getDouble(0), row.getDouble(1)))

    val start = System.currentTimeMillis()


    return (logisticRegressionPredictions.toJavaRDD, start)
  }

}
