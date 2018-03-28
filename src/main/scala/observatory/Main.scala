package observatory

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
object Main extends App {
  val conf = new SparkConf().setMaster("local").setAppName("CategoryCounter")
  val sc = new SparkContext(conf)
  val ss = SparkSession.builder().getOrCreate()

  //Logger.getLogger("org.apache.spark").setLevel(Level.WARN)


}
