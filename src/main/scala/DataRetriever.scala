package isr.project
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._
/**
  * Created by Eric on 11/8/2016.
  */
case class Tweet(id: String, tweetText: String, label: Option[Double] = None)
object DataRetriever {
  var _tableName: String = "ideal-cs5604f16" /*"ideal-cs5604f16-fake"*/
  var _colFam : String = "tweet"
  var _col : String = "cleantext" /*"text"*/


  def retrieveTweets(collectionID: String, sc : SparkContext): RDD[Tweet] = {
    //implicit val config = HBaseConfig()
    val interactor = new HBaseInteraction(_tableName)
    println("MAKING INTERACTOR")
    /*val scanner = new Scan(Bytes.toBytes(collectionID), Bytes.toBytes(collectionID + '0'))
    val cols = Map(
      _colFam -> Set(_col)
    )*/
    //val rdd = sc.hbase[String](_tableName,cols,scanner)
    val result  = interactor.getRowsBetweenPrefix(collectionID, _colFam, _col)
    sc.parallelize(result.iterator().map(r => rowToTweetConverter(r)).toList)
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

