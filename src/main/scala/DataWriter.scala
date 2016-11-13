package isr.project
import org.apache.spark.rdd.RDD
import unicredit.spark.hbase._
/**
  * Created by Eric on 11/9/2016.
  */
object DataWriter {

  def writeTweets(tweets: RDD[Tweet]): Unit ={
    val _tableName: String = "cla-test-table"
    val _colFam : String = "cla-col-fam"
    val _col : String = "classification"
    implicit val config = HBaseConfig()
    val headers = Seq(_col)
    val rdd: RDD[(String, Seq[String])] = tweets.map({tweet => tweet.id -> Seq(labelMapper(tweet.label.getOrElse(999999.0)))})
    rdd.toHBaseBulk(_tableName, _colFam, headers)
    //val interactor = new HBaseInteraction(_tableName)
    //tweets.collect.foreach(tweet => writeTweetToDatabase(tweet,interactor, _colFam, _col))
    println("Wrote to database " + tweets.count() + " tweets")
 }
  def writeTweetToDatabase(tweet : Tweet, interactor: HBaseInteraction, colFam: String, col: String): Unit ={
    interactor.putValueAt(colFam,col,tweet.id,labelMapper(tweet.label.getOrElse(9999999.0)))
  }


  def labelMapper(label:Double) : String= {
    val map = Map(0.0 -> "NewYorkFirefighterShooting", 1.0->"ChinaFactoryExplosion", 2.0->"KentuckyAccidentalChildShooting",
      3.0->"ManhattanBuildingExplosion", 4.0->"NewtownSchoolShooting", 5.0->"HurricaneSandy", 6.0->"HurricaneArthur",
      7.0->"HurricaneIsaac", 8.0->"TexasFertilizerExplosion")
    map.getOrElse(label,"CouldNotClassify")
  }
}

