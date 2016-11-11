package isr.project
import isr.project.{HBaseInteraction, Tweet}
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
/**
  * Created by Eric on 11/9/2016.
  */
object DataWriter {

  def writeTweets(tweets: RDD[Tweet]): Unit ={
    val _tableName: String = "cla-test-table"
    val _colFam : String = "cla-col-fam"
    val _col : String = "classification"
    //tweets.foreach(println)
    val interactor = new HBaseInteraction(_tableName)
    tweets.collect.foreach(tweet => writeTweetToDatabase(tweet,interactor, _colFam, _col))
    /*tweets.foreachPartition((tweets) => {
      val interactor = new HBaseInteraction(_tableName)
      println("MAKING INTERACTOR for table: " + _tableName)
      tweets.foreach(tweet => writeTweetToDatabase(tweet,interactor, _colFam, _col))
    })*/
    println("Wrote to database " + tweets.count() + " tweets") 
    //tweets.foreach(println) 
 }
  def writeTweetToDatabase(tweet : Tweet, interactor: HBaseInteraction, colFam: String, col: String): Unit ={
//	  println(Bytes.toBytes(tweet.label.getOrElse(99.0).toString))
    interactor.putValueAt(colFam,col,tweet.id,tweet.label.getOrElse(99.0).toString)
  }
}

