package isr.project
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{BinaryComparator, CompareFilter, SingleColumnValueFilter}
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.JavaConversions._
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.SparkContext
import java.io.{ByteArrayOutputStream, DataOutputStream}
import java.lang.String

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.Base64
/**
  * Created by Eric on 11/8/2016.
  */
case class Tweet(id: String, tweetText: String, label: Option[Double] = None)
object DataRetriever {
  def convertScanToString(scan: Scan): String = {
    val proto = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(proto.toByteArray)
  }
  var _tableName: String = "ideal-cs5604f16" /*"ideal-cs5604f16-fake"*/
  var _colFam : String = "tweet"
  var _col : String = "cleantext" /*"text"*/
  def retrieveTweets(collectionID: String, sc : SparkContext): RDD[Tweet] = {
    //val sc = new SparkContext(/*conf*/)
    /* val hbaseConfig = HBaseConfiguration.create()
    hbaseConfig.set(TableInputFormat.INPUT_TABLE, _tableName)
    val scan = new Scan(Bytes.toBytes(collectionID), Bytes.toBytes(collectionID + '0'))
    hbaseConfig.set(TableInputFormat.SCAN, convertScanToString(scan))
    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConfig,classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result] )
    hbaseRdd.map((x)=> rowToTweetConverter(x._2))
    */val interactor = new HBaseInteraction(_tableName)
        println("MAKING INTERACTOR")
	    val result  = interactor.getRowsBetweenPrefix(collectionID, _colFam, _col)
	        //result.foreach(println)
		    //println("printed")
		       sc.parallelize(result.iterator().map(r => rowToTweetConverter(r)).toList,30)
  }

  def rowToTweetConverter(result : Result): Tweet ={
    val cell = result.getColumnLatestCell(Bytes.toBytes(_colFam), Bytes.toBytes(_col))
    val key = Bytes.toString(cell.getRowArray, cell.getRowOffset, cell.getRowLength)
    val words = Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
    Tweet(key,words)
  }

}

