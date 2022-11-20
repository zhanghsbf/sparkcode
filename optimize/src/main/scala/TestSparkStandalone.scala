import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TestSparkStandalone {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestSparkStandalone")
      .setMaster("spark://192.168.148.129:7077")
      .setJars(List("D:\\CodePlace\\myspark\\target\\myspark-1.0-SNAPSHOT.jar"))
    val spark = new SparkContext(conf)

    val value: RDD[Int] = spark.makeRDD(1 to 5)
    value.foreach(println _)
  }
}
