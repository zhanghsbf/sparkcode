package common

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.SparkContext

import java.net.URI
import scala.collection.mutable.ArrayBuffer

object HDFSUtil {
//  val uri:URI  = new URI("hdfs://192.168.148.129:9000")
  val uri:URI  = new URI("hdfs://ds-bigdata-001:8020")
  // 创建一个配置文件
  val configuration:Configuration  = new Configuration()
  configuration.set("HADOOP_USER_NAME","zhangykun0508")

  // 用户
  val user:String = "zhangykun0508"
  //获取到了客户端对象
  val fs = FileSystem.get(uri, configuration, user)


  /**
   * ls
   * @param path
   * @param recursive
   * @return
   */
  def listFile(path:String, recursive:Boolean = false):ArrayBuffer[LocatedFileStatus] = {
    val value: RemoteIterator[LocatedFileStatus] = fs.listFiles(new Path(path), recursive)
    val fileArr: ArrayBuffer[LocatedFileStatus] = ArrayBuffer[LocatedFileStatus]()
    while(value.hasNext){
      fileArr += value.next()
    }
    fileArr
  }

  /**
   * rm
   * @param path
   * @param recursive
   * @return
   */
  def removeFile(path:String, recursive:Boolean = false):Boolean = {
    fs.delete(new Path(path), recursive)
  }
}