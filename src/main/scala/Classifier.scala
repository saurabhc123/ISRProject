/**
 * Created by saur6410 on 10/2/16.
 */

package isr.project

import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, SVMWithSGD}

object Classifier {

  def getAlgorithm(algo: String, iterations: Int, stepSize: Double, regParam: Double) = algo match {
    case "logbfgs" => {
      val algo = new LogisticRegressionWithLBFGS()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setRegParam(regParam)
      algo
    }
    case "svm" => {
      val algo = new SVMWithSGD()
      algo.setIntercept(true).optimizer.setNumIterations(iterations).setStepSize(stepSize).setRegParam(regParam)
      algo
    }
  }

}
