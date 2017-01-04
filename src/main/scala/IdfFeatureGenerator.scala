package isr.project


import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.rdd.RDD

/**
 * Created by saur6410 on 12/24/16.
 */
class IdfFeatureGenerator() {


  //var _tweets = tweets
  //var _idfModel : IDFModel = null

  def InitializeIDF(tweets: RDD[Tweet]): (IDFModel, HashingTF) = {

    // Load documents (one per line).
    val documents: RDD[Seq[String]] = tweets.map(tweet => tweet.tweetText.split(" ").toSeq)
    println(documents.first.take(20))
    val dim = math.pow(2, 16).toInt
    val hashingTF = new HashingTF(dim)
    val tf: RDD[linalg.Vector] = hashingTF.transform(documents)
    val sampleString = "west texas tighter ever year fertilizer plant explosion devastating zen"
    //val sampleString = "abc"
    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
 //   tf.cache()
    val idfModel = new IDF().fit(tf)

    /*val sampleStringHash = hashingTF.transform(sampleString.split(" "))
    val sampleStringFeatures = idfModel.transform(sampleStringHash)
    //val v = tf.first.asInstanceOf[SV]
    val v = sampleStringFeatures.asInstanceOf[SV] //hashingTF.transform(sampleString.split(" ")).asInstanceOf[SV]
    println(v.size)
    println(v.values.size)
    println(v.values.take(20).toSeq)
    println(v.indices.take(20).toSeq)*/


    return (idfModel, hashingTF)
    //val tfidf: RDD[linalg.Vector] = idf.transform(tf)

  }

  def GetIdfForWord(tweetText: String, idfModel: IDFModel): linalg.Vector = {
    if (idfModel == null)
      throw new Exception("IDF dictionary not initialized")
    //Everything is fine. Return the IDF value of the word.
    val hashingTF = new HashingTF(160)
    val features = hashingTF.transform(tweetText)
    val wordFeatures = idfModel.transform(features)
    return wordFeatures
  }

}
