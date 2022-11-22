import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkContext}

object WordCount{
  def main(args: Array[String]):Unit = {

    val session: SparkSession = SparkSession.builder().appName("zyk-spark-submittest").getOrCreate()
    val spark: SparkContext = session.sparkContext


    // wordcount逻辑开始
    val inPath: String = "hdfs:///user/zhangykun0508/exe.log"
    val outPath: String = "hdfs:///user/zhangykun0508/wc.out"

    val file: RDD[String] = spark.textFile(inPath)

    val result: RDD[(String, Int)] = file.flatMap(a => a.split(" ")).map(a => (a, 1)).reduceByKey(_ + _)

    result.saveAsTextFile(outPath)
  }
}
