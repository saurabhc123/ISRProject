package scala
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

/**
 * Created by saur6410 on 10/2/16.
 */

class Greeter(message: RDD[Array[String]]) {
    def SayHi() = println(message)
}

class FeatureGenerator(message: RDD[Array[String]]) {

  val fpmWords = message
  val tweetLength:Int = 70



  def getFeatures(featureType: String, documentBody: String) = featureType match {
    case "fpm" => {
      //Load the FPM List of Lists
      //Initialize a new array of booleans based on the FPM List length
      //Iterate through the FPM List
      //For each fpm record, find the intersection with the documentBody list(maybe?)
        //If the intersection set size is the same as the fpm record size, set the boolean to 1.0
        //Add the boolean to the array of booleans that is being composed.
      //Return the array of booleans as a Dense Vector

      //ToDo: Remove this stubbed implementation
      var issac, hurricane, weather = 0.0
      if(documentBody.contains("issac"))
        issac = 1.0
      if(documentBody.contains("hurricane"))
        hurricane = 1.0
      if(documentBody.contains("weather"))
        weather = 1.0

      Vectors.dense(issac, hurricane, weather)
    }
    case "wcp" => {

      Vectors.dense(1.0, 0.0, 3.0)
    }
    case "word2vec" => {
        val features = documentBody.split(" ").map(word => WordVectorGenerator.getAveragedWordVector(word))
        Vectors.dense(PadFeatureArray(features))
    }
  }


  def PadFeatureArray(features: Array[Double]): Array[Double] = {

    val paddedFeatures = new Array[Double](tweetLength)
    for (i <- 1 to features.length) {
        paddedFeatures(i - 1) = features(i -1)
    }
    return paddedFeatures
  }

}
