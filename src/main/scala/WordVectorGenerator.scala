package scala

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{Word2VecModel, Word2Vec}


/**
 * Created by saur6410 on 10/13/16.
 */


object WordVectorGenerator {

  var _classModel : Word2VecModel = null
  val _word2VecModelLength = 70

  def generateWordVector(inputFilename: String, sc: SparkContext):Unit = {
    val input = sc.textFile(inputFilename).map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()
    word2vec.setVectorSize(_word2VecModelLength)
    val model = word2vec.fit(input)
    _classModel = model

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
