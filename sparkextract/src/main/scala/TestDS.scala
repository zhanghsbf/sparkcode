import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object TestDS {
    def main(args: Array[String]):Unit = {
      // spark配置新建
      val sparkConf = new SparkConf().setAppName("Operator")
      // spark上下文对象
      val spark: SparkContext = SparkContext.getOrCreate(sparkConf)

      val file: RDD[Int] = spark.makeRDD(Seq(1,2,3,4,5))
      file.foreach(x => println(x))
    }
}
