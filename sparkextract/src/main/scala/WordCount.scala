import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount{
  def main(args: Array[String]):Unit = {
    // spark配置新建
    val sparkConf = new SparkConf().setAppName("Operator")
    // spark上下文对象
    val spark: SparkContext = SparkContext.getOrCreate(sparkConf)

    // wordcount逻辑开始
    val inPath: String = "hdfs:///user/zhangykun0508/exe.log"
    val outPath: String = "hdfs:///user/zhangykun0508/wc.out"

    val file: RDD[String] = spark.textFile(inPath)

    val result: RDD[(String, Int)] = file.flatMap(a => a.split(" ")).map(a => (a, 1)).reduceByKey(_ + _)

    result.saveAsTextFile(outPath)
  }
}
