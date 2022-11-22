
import common.ApplicationContext
import common.ApplicationContext.session
import common.ErrorType.JSON_LACK_SCHEMA_ERR
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.{DecimalType, StructField}
import schema.GameLogSchema

import java.net.URI
import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util
import scala.collection.mutable.ArrayBuffer

object JustTest {
  def main(args: Array[String]): Unit = {
//    Array(1,2,3,4,5).foreach(_ => println _)
    System.setProperty("HADOOP_USER_NAME", "zhangykun0508")
    val session: SparkSession = SparkSession.builder().appName("zyk-spark-extract").enableHiveSupport().master("local[*]").getOrCreate()
    val spark: SparkContext = session.sparkContext
    val catalog = ApplicationContext.session.sessionState.catalog

    val df: DataFrame = session.read.schema(GameLogSchema.SCHEMA_ROLE_RECHARGE).json("file:///D:/CodePlace/sparkcode/sparkextract/data/test.json")
    val fields  = ArrayBuffer() ++= df.schema.fields.map(field => field.name)

    if(catalog.tableExists(TableIdentifier("zyk_role_recharge",Some("ds_spark")))){
      // 如果表存在，则使用 insertInto 插入
      session.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      session.conf.set("spark.sql.parquet.writeLegacyFormat", true)
      df.selectExpr(fields += s"'test' as part_dt": _*)
        .write
        .format("parquet")
        .mode("overwrite")
        .insertInto("ds_spark.zyk_role_recharge")
    } else {
      df.selectExpr(fields += s"'test' as part_dt": _*)
        .write
        .format("parquet")
        .mode("overwrite")
        .partitionBy("part_dt")
        .saveAsTable("ds_spark.zyk_role_recharge")
    }

  }

  def getPartition(key: Any): Int = {//确定数据进入分区的具体策略
    val keyStr = key.toString
    val keyTag = keyStr.substring(keyStr.length - 1, keyStr.length)
    println(keyTag)
    keyTag.toInt % 4
  }
}
