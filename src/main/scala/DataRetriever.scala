package isr.project
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._

/**
  * Created by Eric on 11/8/2016.
  */
case class Tweet(id: String, tweetText: String, label: Option[Double] = None)
object DataRetriever {
  var _tableName: String = "ideal-tweet"
  var _colFam : String = "tweet"
  var _col : String = "cleantext"
  def retrieveTweets(collectionID: String): Iterator[Tweet] ={
    val interactor = new HBaseInteraction(_tableName)
    println("MAKING INTERACTOR")
    val result  = interactor.getRowsBetweenPrefix(collectionID, _colFam, _col)
    //result.foreach(println)
    //println("printed")
   result.iterator().map(r => rowToTweetConverter(r))
  }


  def rowToTweetConverter(result : Result): Tweet ={
    val cell = result.getColumnLatestCell(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
    val key = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
    val words = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
    Tweet(key,words)
  }

}

