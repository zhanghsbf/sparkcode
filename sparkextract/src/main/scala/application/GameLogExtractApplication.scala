package application

import cleaner.JsonCleaner
import common.{ApplicationContext, HDFSUtil}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}
import schema.GameLogSchema
import sink.HiveSink

import java.util.concurrent.CountDownLatch

class GameLogExtractApplication

object GameLogExtractApplication extends BaseApplication{
  private val logger: Logger = LoggerFactory.getLogger(classOf[GameLogExtractApplication])

  var partDt = "9999-12-31"
  var gameLogDir = "D:\\CodePlace\\sparkextract\\data\\part_date=test"
  val baseSavePath = s"hdfs://ds-bigdata-001:8020/user/zhangykun0508/jsonTrash/gameLog/"
  val keyField = "event_name"

  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("按顺序传入两个参数: 日志路径, 数据日期(yyyy-MM-dd)")
      System.exit(1)
    }

    gameLogDir = args(0)
    partDt = args(1)
    run()
  }

  override def process(): Unit = {
    val savePath = baseSavePath + partDt
    // 重跑删除目录 todo 成功失败判断
    HDFSUtil.removeFile(savePath, true)
    // 清理非json及空串
    val textRDD: RDD[String] = ApplicationContext.spark.textFile(gameLogDir)

    logger.info("开始数据清洗！")
    // 执行数据清洗，以json对应的表类型或错误类型，将结果落盘hdfs
    textRDD.map(content => {
      val contentType: String = JsonCleaner.clean(content, keyField, GameLogSchema.schemaMap)
      (contentType, content)
    }).partitionBy(new HashPartitioner(10))
      .saveAsHadoopFile(savePath, classOf[String], classOf[String], classOf[RDDMultipleTextOutputFormat])
    logger.info("数据清洗完成！")

    // 获取要接入的文件
    val cleanFiles = HDFSUtil.listFile(savePath).filterNot(x =>{
      x.getPath.getName.endsWith("ERR") ||
        x.getPath.getName.equals("_SUCCESS")
    })
    logger.info("清洗后文件读取成功！")

    val latch = new CountDownLatch(cleanFiles.size)
    cleanFiles.foreach(file => {
      ApplicationContext.THREAD_POOL.execute(() => {
        try {
          val fileName: String = file.getPath.getName
          val start: Long = System.currentTimeMillis()
          logger.info(s"${Thread.currentThread().getName} 正在接入文件： ${file.getPath.getName}")

          val schema: Option[StructType] = GameLogSchema.schemaMap.get(fileName)
          schema match {
            case None => logger.error(s"清洗后的文件 $fileName 找不到对应表结构，请检查！")
            case Some(schema) =>
              val fileDF: DataFrame = ApplicationContext.session.read.schema(schema).json(file.getPath.toString)
              HiveSink.dfSaveHiveTablePartDt(fileDF, "ds_spark", s"zyk_$fileName", partDt)
          }
          val cost: Long = (System.currentTimeMillis() - start) / 1000
          logger.info(s"${Thread.currentThread().getName} 完成接入： ${file.getPath.getName} 耗时: $cost 秒")
        } catch {
          case e: Exception => logger.error(e.toString)
        } finally {
          latch.countDown()
        }
      })
    })

    // 防止线程未执行完而spark session提前被关闭
    latch.await()
    logger.info("数据接入全部完成！")
  }

  /**
   * 保存文件至hdfs时，按照分区的key作为文件名
   */
  class RDDMultipleTextOutputFormat  extends MultipleTextOutputFormat[Any, Any] {
    override def generateActualKey(key: Any, value: Any): Any =
      NullWritable.get()

    override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
      key.asInstanceOf[String]

  }
}




