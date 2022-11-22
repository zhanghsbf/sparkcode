package common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}


object ApplicationContext {
  val session: SparkSession = SparkSession.builder().appName("zyk-spark-extract").enableHiveSupport().getOrCreate()
//  System.setProperty("HADOOP_USER_NAME", "zhangykun0508")
//  val session: SparkSession = SparkSession.builder().appName("zyk-spark-extract").enableHiveSupport().master("local[*]").getOrCreate()
  val spark: SparkContext = session.sparkContext

  val THREAD_POOL = new ThreadPoolExecutor(2,4,500,TimeUnit.MILLISECONDS,new LinkedBlockingQueue[Runnable]())

  def close() = {
    session.close()
    THREAD_POOL.shutdown()
  }
}
