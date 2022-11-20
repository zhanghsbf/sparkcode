package dao

import common.ApplicationContext
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


trait BaseDao {

  val catalog = ApplicationContext.session.sessionState.catalog
  ApplicationContext.session.conf.set("spark.sql.sources.partitionOverwriteMode", "DYNAMIC")
  var dataSource:mutable.HashMap[String, String]
  initDataSource()

  def saveJdbcTable(df:DataFrame, tableName:String, cols:ListBuffer[String], saveMode:String = "overwrite"): Unit = {
    df.selectExpr(cols : _*)
      .write
      .format("jdbc")
      .options(dataSource)
      .option("dbtable",tableName)
      .mode(saveMode)
      .save()
  }

  def saveHiveTable(df:DataFrame, dbName:String, tableName:String, cols:ListBuffer[String], partition:String, saveMode:String = "overwrite"): Unit = {
//    println(((s"'$partition'" + " as part_dt") +: cols).mkString(","))
    if(catalog.tableExists(TableIdentifier(tableName,Some(dbName)))){
      df.selectExpr(cols += s"'$partition' as part_dt": _*)
        .write
        .format("parquet")
        .mode(saveMode)
        .insertInto(dbName + "." + tableName)
    } else {
//      df.selectExpr((s"'$partition'" + " as part_dt") +: cols : _*)
      df.selectExpr(cols += s"'$partition' as part_dt": _*)
        .write
        .format("parquet")
        .mode(saveMode)
        .partitionBy("part_dt")
        .saveAsTable(dbName + "." + tableName)
    }
  }


  def initDataSource(): Unit = {
    val map = new mutable.HashMap[String,String]()
    map.put("url","jdbc:mysql://localhost:3306/zyk?useSSL=false")
    map.put("driver","com.mysql.jdbc.Driver")
    map.put("user","root")
    map.put("password","root")
    dataSource = map
  }
}
