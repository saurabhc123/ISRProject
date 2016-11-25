package isr.project

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Word2VecClassifier
import org.apache.spark.rdd.RDD
/**
  * Created by Eric on 11/8/2016.
  */
case class Tweet(id: String, tweetText: String, label: Option[Double] = None)
object DataRetriever {
  val _cachedRecordCount = 50000
  var _tableName: String = "ideal-cs5604f16" /*"ideal-cs5604f16-fake"*/
  var _colFam : String = "tweet"
  var _col : String = "cleantext" /*"text"*/
  var _word2VecModelFilename = "data/word2vec.model"


  def retrieveTweets(collectionID: String, sc : SparkContext): RDD[Tweet] = {
    //implicit val config = HBaseConfig()
    val scan = new Scan(Bytes.toBytes(collectionID), Bytes.toBytes(collectionID + '0'))
    val hbaseConf = HBaseConfiguration.create()
    val table = new HTable(hbaseConf,_tableName)
    scan.addColumn(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
    scan.setCaching(_cachedRecordCount)
    scan.setBatch(100)

    val bcWord2VecModelFilename = sc.broadcast(_word2VecModelFilename)
    val word2vecModel = Word2VecModel.load(sc, bcWord2VecModelFilename.value)
    //Perform a cold start of the model pipeline so that this loading
    //doesn't disrupt the read later.
    val coldTweet = sc.parallelize(Array[Tweet]{ Tweet("id", "Some tweet")})
    val predictedTweets = Word2VecClassifier.predict(coldTweet, sc, word2vecModel)
    predictedTweets.count

    val resultScanner = table.getScanner(scan)
    println(s"Caching Info:${scan.getCaching} Batch Info: ${scan.getBatch}")
    println("Scanning results now.")

    var continueLoop = true
    var totalRecordCount = 0
    while (continueLoop) {
      try {
        println("Getting next batch of results now.")
        val results = resultScanner.next(_cachedRecordCount)
        //var resultTweets = rowToTweetConverter(resultScanner.next(_cachedRecordCount))

        if (results == null)
          continueLoop = false
        else {
          val resultTweets = resultScanner.next(_cachedRecordCount).map(r => rowToTweetConverter(r))
          val rddT = sc.parallelize(resultTweets)
          rddT.cache()
          rddT.repartition(12)
          println("*********** Cleaning the tweets now. *****************")
          val cleanTweets = CleanTweet.clean(rddT, sc)
          println("*********** Predicting the tweets now. *****************")
          val predictedTweets = Word2VecClassifier.predict(cleanTweets, sc, word2vecModel)
          println("*********** Persisting the tweets now. *****************")
          //actualTweets.map(t => println(s"Tweet Text:${t.tweetText} Label:${t.label}"))
          DataWriter.writeTweets(predictedTweets)
          //val actualTweets = predictedTweets.take(1)
          //actualTweets.map(t => println(s"Tweet Text:${t.tweetText} Label:${t.label}"))
          //totalRecordCount = predictedTweets.count().toInt + 1
          //println(s"Tweet Text:${actualTweets} Label:${actualTweets.label}"))
        }
      }
      catch {
        case e: Exception =>
          println(e.printStackTrace())
          println("Exception Encountered")
          println(e.getMessage)
          continueLoop = false
      }

    }

    println(s"Total record count:${totalRecordCount}")
    resultScanner.close()
    //val interactor = new HBaseInteraction(_tableName)

    return null



    /*val scanner = new Scan(Bytes.toBytes(collectionID), Bytes.toBytes(collectionID + '0'))
    val cols = Map(
      _colFam -> Set(_col)
    )*/
    //val rdd = sc.hbase[String](_tableName,cols,scanner)
    //val result  = interactor.getRowsBetweenPrefix(collectionID, _colFam, _col)
    //sc.parallelize(result.iterator().map(r => rowToTweetConverter(r)).toList)
    //rdd.map(v => Tweet(v._1, v._2.getOrElse(_colFam, Map()).getOrElse(_col, ""))).foreach(println)
    //rdd.map(v => Tweet(v._1, v._2.getOrElse(_colFam, Map()).getOrElse(_col, "")))/*.repartition(sc.defaultParallelism)*/.filter(tweet => tweet.tweetText.trim.isEmpty)
  }

  def rowToTweetConverter(result : Result): Tweet ={
    val cell = result.getColumnLatestCell(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
    val key = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
    val words = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
    Tweet(key,words)
  }

}

