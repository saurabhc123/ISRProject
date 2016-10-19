package org.apache.spark.mllib.linalg

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV, Vector => BV}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.rdd.RDD

import scala.util.Try


/**
 * Created by saur6410 on 10/13/16.
 */


object WordVectorGenerator {

  var _classModel : Word2VecModel = null
  val _word2VecModelLength = 70

  def generateWordVector(inputFilename: String, sc: SparkContext):Unit = {
    //lists of list of words
    val input = sc.textFile(inputFilename).map(line => line.split(" "))



    val samplePairs = input.map(s => s).cache()
    val inputWords: RDD[(Iterable[String])] = samplePairs.map(x => x.toIterable)
    println("Start Training Word2Vec --->")

    val word2vec = new Word2Vec()
    word2vec.setVectorSize(_word2VecModelLength)
    val word2VecModel = word2vec.fit(inputWords)
    _classModel = word2VecModel

    def wordFeatures(words: Iterable[String]): Iterable[Vector] = words.map(w => Try(word2VecModel.transform(w))).filter(_.isSuccess).map(_.get)

    def avgWordFeatures(wordFeatures: Iterable[Vector]): Vector = Vectors.fromBreeze(wordFeatures.map(_.asBreeze).reduceLeft(_ + _) / wordFeatures.size.toDouble)

    // Create a feature vectors
    val wordFeaturePair = inputWords map wordFeatures
    val avgWordFeaturesPair = wordFeaturePair map avgWordFeatures
    /*val featuresPair = avgWordFeaturesPair join samplePairs mapValues {
    case (features, s ) => LabeledPoint(s, features)
     }


    val trainingSet = featuresPair.values*/

    /*val synonyms = model.findSynonyms("loan", 5)
    val loanVector = model.transform("loan")
    val newVector = model.transform("new")
    println(loanVector.size)
    println(newVector.size)

    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym $cosineSimilarity")
}*/
  }


  def getAveragedWordVector(word: String):Double = {
    var sum = 0.0
    try {
      val transformedVector = _classModel.transform(word)
      transformedVector.toArray.map(x => sum = sum + x)
      //println(word ,":" , sum)
    } catch {
      case _ => sum = 0.0
    }

    return sum/_word2VecModelLength
  }

}
