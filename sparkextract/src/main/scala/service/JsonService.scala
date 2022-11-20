package service

import org.apache.spark.rdd.RDD

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.parsing.json._



object JsonService extends BaseService {

  private var baseTrashPath:String = "hdfs:///user/zhangykun0508/jsonTrash/unformated/"
//  private var baseTrashPath:String = "data/jsonTrash/unformated/"
  // 202201031012
  private val dateFormat = new SimpleDateFormat("yyyyMMddhhmm")

  override def process(): Unit = {

  }

  def cleanJson(rdd:RDD[String]):RDD[String] = {
    rdd.cache()
    saveTrash(rdd)
    rdd.filter(a => isJson(a))
  }

  def saveTrash(rdd:RDD[String]) = {
    val trash: RDD[String] = rdd.filter(a => !isJson(a))
    val trashPath = baseTrashPath + dateFormat.format(new Date())
    trash.saveAsTextFile(trashPath)
  }

  // todo 还需要判断json的字段是否合法，符合schema要求   spark
  def isJson(str:String): Boolean = {
    JSON.parseRaw(str) match {
      case Some(s) => true
      case None => false
    }
  }

}
