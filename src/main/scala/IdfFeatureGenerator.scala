package isr.project


import org.apache.spark.mllib.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.mllib.linalg
import org.apache.spark.rdd.RDD

/**
 * Created by saur6410 on 12/24/16.
 */
class IdfFeatureGenerator() {


  //var _tweets = tweets
  //var _idfModel : IDFModel = null

  def InitializeIDF(tweets: RDD[Tweet]): IDFModel = {

    // Load documents (one per line).
    val documents: RDD[Seq[String]] = tweets.map(tweet => tweet.tweetText.split(" ").toSeq)

    val hashingTF = new HashingTF()
    val tf: RDD[linalg.Vector] = hashingTF.transform(documents)

    // While applying HashingTF only needs a single pass to the data, applying IDF needs two passes:
    // First to compute the IDF vector and second to scale the term frequencies by IDF.
    tf.cache()
    val idfModel = new IDF(minDocFreq = 1).fit(tf)
    return idfModel
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
