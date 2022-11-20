package common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


object ApplicationContext {
//  val session: SparkSession = SparkSession.builder().appName("zyk-spark-extract").enableHiveSupport().getOrCreate()
  val session: SparkSession = SparkSession.builder().appName("zyk-spark-extract").master("local[*]").getOrCreate()
  val spark: SparkContext = session.sparkContext
}
