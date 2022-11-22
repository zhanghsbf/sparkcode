package sink

import com.alibaba.fastjson2.{JSON, JSONObject}
import common.ApplicationContext
import org.apache.commons.logging.LogFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.types.StructType

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class HiveSink{
//  val log = LogFactory.getLog(classOf[HiveSink])
  val catalog = ApplicationContext.session.sessionState.catalog
  import common.ApplicationContext.session.implicits._

  def rddSaveHiveTablePartDt(rdd:RDD[String], dbName:String, tableName:String, schema:StructType, partDt:String, saveMode:String = "overwrite"): Unit = {
    val rowRDD: RDD[Row] = rdd.map(str => {
      val j: JSONObject = JSON.parseObject(str)
      Row.fromSeq(j.values.toArray)
    })

    val df: DataFrame = ApplicationContext.session.createDataFrame(rowRDD, schema)
//    log.info("函数rddSaveHiveTablePartDt 解析DataFrame完成")
    dfSaveHiveTablePartDt(df:DataFrame, dbName, tableName, partDt, saveMode)
  }

  def dfSaveHiveTablePartDt(df:DataFrame, dbName:String, tableName:String, partDt:String, saveMode:String = "overwrite"): Unit = {
    val fields  = ArrayBuffer() ++= df.schema.fields.map(field => field.name)
//    log.info("函数 dfSaveHiveTablePartDt fields" + fields)

//    df.write.save(s"file:///D:/CodePlace/sparkcode/datadata/result/$tableName")

    if(catalog.tableExists(TableIdentifier(tableName,Some(dbName)))){
      ApplicationContext.session.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
      df.selectExpr(fields += s"'$partDt' as part_dt": _*)
        .write
        .format("parquet")
        .mode(saveMode)
        .insertInto(dbName + "." + tableName)
    } else {
      df.selectExpr(fields += s"'$partDt' as part_dt": _*)
        .write
        .format("parquet")
        .mode(saveMode)
        .partitionBy("part_dt")
        .saveAsTable(dbName + "." + tableName)
    }
  }
}

object HiveSink extends HiveSink