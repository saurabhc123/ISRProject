package isr.project

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.linalg.Word2VecClassifier

import scala.collection.JavaConversions._
/**
  * Created by Eric on 11/8/2016.
  */
case class Tweet(id: String, tweetText: String, label: Option[Double] = None)
object DataRetriever {
  val _lrModelFilename = "data/lrclassifier.model"
  var _cachedRecordCount = 50
  var _tableName: String = "ideal-cs5604f16" /*"ideal-cs5604f16-fake"*/
  var _colFam : String = "tweet"
  var _col : String = "cleantext" /*"text"*/
  var _word2VecModelFilename = "data/word2vec.model"

  def retrieveTweets(args: Array[String], sc: SparkContext): RDD[Tweet] = {
    //implicit val config = HBaseConfig()

    val collectionID = args(0)
    if (args.length >= 2)
      _cachedRecordCount = args(2).toInt
    val scan = new Scan(Bytes.toBytes(collectionID), Bytes.toBytes(collectionID + '0'))
    val hbaseConf = HBaseConfiguration.create()
    val table = new HTable(hbaseConf,_tableName)
    scan.addColumn(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
    scan.setCaching(_cachedRecordCount)
    scan.setBatch(100)

    val bcWord2VecModelFilename = sc.broadcast(_word2VecModelFilename)
    val bcLRClassifierModelFilename = sc.broadcast(_lrModelFilename)
    val word2vecModel = Word2VecModel.load(sc, bcWord2VecModelFilename.value)
    val logisticRegressionModel = LogisticRegressionModel.load(sc, bcLRClassifierModelFilename.value)
    println(s"Classifier Model file found:$bcLRClassifierModelFilename. Loading model.")
    //Perform a cold start of the model pipeline so that this loading
    //doesn't disrupt the read later.
    val coldTweet = sc.parallelize(Array[Tweet]{ Tweet("id", "Some tweet")})
    val predictedTweets = Word2VecClassifier.predict(coldTweet, sc, word2vecModel, logisticRegressionModel)
    predictedTweets.count



    val resultScanner = table.getScanner(scan)
    println(s"Caching Info:${scan.getCaching} Batch Info: ${scan.getBatch}")
    println("Scanning results now.")

    var continueLoop = true
    var totalRecordCount: Long = 0
    while (continueLoop) {
      try {
        println("Getting next batch of results now.")
        val start = System.currentTimeMillis()
        val results = resultScanner.next(_cachedRecordCount)
        //var resultTweets = rowToTweetConverter(resultScanner.next(_cachedRecordCount))

        if (results == null || results.length == 0)
          continueLoop = false
        else {
          println(s"Result Length:${results.length}")
          val resultTweets = results.map(r => rowToTweetConverter(r))
          val rddT = sc.parallelize(resultTweets)
          rddT.cache()
          rddT.repartition(12)
          println("*********** Cleaning the tweets now. *****************")
          val cleanTweets = CleanTweet.clean(rddT, sc)
          println("*********** Predicting the tweets now. *****************")
          val predictedTweets = Word2VecClassifier.predict(cleanTweets, sc, word2vecModel, logisticRegressionModel)
          println("*********** Persisting the tweets now. *****************")
          //actualTweets.map(t => println(s"Tweet Text:${t.tweetText} Label:${t.label}"))


          val firstTweet = predictedTweets.take(1)
          firstTweet.foreach(actualTweets =>
            println(s"Tweet Text:${actualTweets.tweetText} Label:${actualTweets.label}"))
          //val i = firstTweet.length
          //predictedTweets.collect().take(1).foreach(s => s"Tweet Text:${s.tweetText} Label:${s.label}")
          predictedTweets.cache()
          val batchTweetCount = predictedTweets.count()
          val repartitionedPredictions = predictedTweets.repartition(12)
          println(s"The amount of tweets to be written is $batchTweetCount")
          DataWriter.writeTweets(repartitionedPredictions)
          val end = System.currentTimeMillis()
          totalRecordCount += batchTweetCount
          println(s"Took ${(end-start)/1000.0} seconds for This Batch.")
          println(s"This batch had $batchTweetCount tweets. We have processed $totalRecordCount tweets overall")
          continueLoop = false
          //puts.collect()
          //val recordCount = puts.count()
//          println("Wrote to database " + recordCount + " tweets")

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

    println(s"Total record count:$totalRecordCount")
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

  def retrieveTrainingTweetsFromFile(fileName:String, sc : SparkContext) : RDD[Tweet] = {
    val lines = sc.textFile(fileName)
    lines.map(line=> Tweet(line.split('|')(1), line.split('|')(2), Option(line.split('|')(0).toDouble))).filter(tweet => tweet.label.isDefined)
  }

  def getTrainingTweets(sc:SparkContext): RDD[Tweet] = {
    val _tableName: String = "cs5604-f16-cla-training"
    val _textColFam: String = "training-tweet"
    val _labelCol: String = "label"
    val _textCol : String = "text"
    val connection = ConnectionFactory.createConnection()
    val table = connection.getTable(TableName.valueOf(_tableName))
    val scanner = new Scan()
    scanner.addColumn(Bytes.toBytes(_textColFam), Bytes.toBytes(_labelCol))
    scanner.addColumn(Bytes.toBytes(_textColFam), Bytes.toBytes(_textCol))
    sc.parallelize(table.getScanner(scanner).map(result => {
      val labcell = result.getColumnLatestCell(Bytes.toBytes(_textColFam), Bytes.toBytes(_labelCol))
      val textcell = result.getColumnLatestCell(Bytes.toBytes(_textColFam), Bytes.toBytes(_textCol))
      val key = Bytes.toString(labcell.getRowArray, labcell.getRowOffset, labcell.getRowLength)
      val words = Bytes.toString(textcell.getValueArray, textcell.getValueOffset, textcell.getValueLength)
      val label = Bytes.toString(labcell.getValueArray, labcell.getValueOffset, labcell.getValueLength).toDouble
      Tweet(key,words,Option(label))
    }).toList)
  }

}

