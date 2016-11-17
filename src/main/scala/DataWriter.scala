package isr.project
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import unicredit.spark.hbase._
/**
  * Created by Eric on 11/9/2016.
  */
object DataWriter {

  def writeTweets(tweets: RDD[Tweet]): Unit ={
    val _tableName: String = "cla-test-table"/*"ideal-cs5604f16"*/
    val _colFam : String = "cla-col-fam"/*"clean-tweet"*/
    val _col : String = "classification"/*"real-world-events"*/
    //implicit val config = HBaseConfig()
    //val headers = Seq(_col)
    //val rdd: RDD[(String, Seq[String])] = tweets.map({tweet => tweet.id -> Seq(labelMapper(tweet.label.getOrElse(999999.0)))})
    //rdd.toHBase(_tableName, _colFam, headers)

    tweets.repartition(120).map(tweet => {

      val hbaseConf = HBaseConfiguration.create()
      val table = new HTable(hbaseConf,_tableName)
      table.put(writeTweetToDatabase(tweet,_colFam,_col,table))
    }
    ).collect()
    //val interactor = new HBaseInteraction(_tableName)
    //tweets.collect.foreach(tweet => writeTweetToDatabase(tweet,interactor, _colFam, _col))
    //println("Wrote to database " + tweets.count() + " tweets")
 }
  def writeTweetToDatabase(tweet : Tweet, colFam: String, col: String, table: HTable): Put ={
    putValueAt(colFam,col,tweet.id,labelMapper(tweet.label.getOrElse(9999999.0)), table)
  }

  def putValueAt(columnFamily: String, column: String, rowKey: String, value: String, table: HTable) : Put = {
    // Make a new put object to handle adding data to the table
    // https://hbase.apache.org/apidocs/org/apache/hadoop/hbase/client/Put.html
    val put = new Put(Bytes.toBytes(rowKey))

    // add data to the put
    put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value))

    // put the data in the table
    put
  }


  def labelMapper(label:Double) : String= {
    val map = Map(0.0 -> "NewYorkFirefighterShooting", 1.0->"ChinaFactoryExplosion", 2.0->"KentuckyAccidentalChildShooting",
      3.0->"ManhattanBuildingExplosion", 4.0->"NewtownSchoolShooting", 5.0->"HurricaneSandy", 6.0->"HurricaneArthur",
      7.0->"HurricaneIsaac", 8.0->"TexasFertilizerExplosion")
    map.getOrElse(label,"CouldNotClassify")
  }
}

