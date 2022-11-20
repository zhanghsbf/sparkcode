package service

import common.ApplicationContext
import org.apache.spark.rdd.RDD

class WordCountService extends BaseService {

  override def init(): Unit = {

  }

  override def process(): Unit = {
//    val inPath: String = "hdfs:///user/zhangykun0508/exe.log"
    val inPath: String = "data/1.log"
//    val outPath: String = "hdfs:///user/zhangykun0508/wc.out"
    val outPath: String = "data/wcout"

    val file: RDD[String] = ApplicationContext.spark.textFile(inPath)
    val result: RDD[(String, Int)] = file.flatMap(a => a.split(" ")).map(a => (a, 1)).reduceByKey(_ + _)

    result.saveAsTextFile(outPath)
  }

  override def destroy(): Unit = {

  }
}
