package isr.project
import java.io.{ByteArrayOutputStream, IOException}

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.util.Base64

import scala.collection.JavaConversions._
/**
  * Created by Eric on 11/8/2016.
  */
case class Tweet(id: String, tweetText: String, label: Option[Double] = None)
object DataRetriever {
  val _tableName: String = "ideal-cs5604f16" /*"ideal-cs5604f16-fake"*/
  val _colFam : String = "tweet"
  val _col : String = "cleantext" /*"text"*/
  val _partitionCount = 120
  def convertScanToString(scan: Scan): String = {
    val proto: ClientProtos.Scan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }

  def retrieveTweets(collectionID: String, sc : SparkContext): RDD[Tweet] = {
    getTweetRDD(collectionID, sc)
  }

  def getTweetRDD(prefix: String, sc : SparkContext): RDD[Tweet] = {
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, _tableName)
    val scanner = new Scan(Bytes.toBytes(prefix), Bytes.toBytes(prefix + '0'))
    scanner.addColumn(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
    conf.set(TableInputFormat.SCAN, convertScanToString(scanner))

    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]).map(e => e._2)
    hBaseRDD.repartition(_partitionCount).map(e => {
      val cell = e.getColumnLatestCell(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
      val key = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
      val words = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
      Tweet(key,words)
      })
  }

  def rowToTweetConverter(result : Result): Tweet ={
    val cell = result.getColumnLatestCell(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
    val key = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
    val words = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
    Tweet(key,words)
  }

}

