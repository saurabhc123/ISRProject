package isr.project
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import unicredit.spark.hbase._

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
    implicit val config = HBaseConfig()
    val interactor = new HBaseInteraction(_tableName)
    println("MAKING INTERACTOR")
    val scanner = new Scan(Bytes.toBytes(collectionID), Bytes.toBytes(collectionID + '0'))
    val cols = Map(
      _colFam -> Set(_col)
    )
    val rdd = sc.hbase[String](_tableName,cols,scanner)
    rdd.map(v => Tweet(v._1, v._2.getOrElse(_colFam, Map()).getOrElse(_col, ""))).foreach(println)
    rdd.map(v => Tweet(v._1, v._2.getOrElse(_colFam, Map()).getOrElse(_col, "")))
  }

  def rowToTweetConverter(result : Result): Tweet ={
    val cell = result.getColumnLatestCell(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
    val key = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
    val words = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
    Tweet(key,words)
  }

}

