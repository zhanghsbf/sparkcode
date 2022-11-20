package service

import common.ApplicationContext
import dao.GameLogDao
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import com.alibaba.fastjson2.JSON
import com.alibaba.fastjson2.JSONArray
import com.alibaba.fastjson2.JSONObject
import org.apache.spark.sql.types.StructType
import common.ErrorType._
import schema.GameLogSchema._

import java.util
import java.util.Map




class EventService extends Serializable {
  import ApplicationContext.session.implicits._

  def extract(path:String, partDt:String) = {
    // 清理非json及空串
    val textRDD: RDD[String] = JsonService.cleanJson(ApplicationContext.spark.textFile(path))

    ApplicationContext.session.read.json(textRDD.toDS()).printSchema()
    ApplicationContext.session.read.json(textRDD.toDS()).schema
    // todo 改成打上异常tag或者直接落到一张异常表
    val json: DataFrame = ApplicationContext.session.read.json(textRDD.toDS())

    // 1. 通过RDD分区把不同schema的数据  -> 临时落盘
    // 2. hdfs

    // 清理无event_name字段的数据
    val value: Dataset[Row] = json.filter($"event_name" isNotNull)

    // 清洗后的数据落表
    val dao = new GameLogDao(value)
//        dao.saveByType()
    dao.saveHiveByType(partDt)
  }

  def extract2(path:String, partDt:String) = {
    // 清理非json及空串
    val textRDD: RDD[String] = ApplicationContext.spark.textFile(path)
    textRDD.map(x => {
      var typ:String = ""
      var value:JSONObject = null

      // 1 如果是空串或者格式非法
      try{
        if(x == "") {
          typ = JSON_BLANK_ERR
        } else {
          value = JSON.parseObject(x)
        }
      } catch{
        case e:Exception => typ = JSON_SYNTAX_ERR
      }
      val set: util.Set[Map.Entry[String, AnyRef]] = value.entrySet()
      // 判断json是否符合对应schema规范
      value.remove("event_name") match {
        case "" => typ = JSON_LACK_EVENT_NAME_ERR
        case ROLE_RECHARGE => typ = getSchemaType(value, ROLE_RECHARGE, role_recharge)
        case _ => typ = JSON_LACK_EVENT_NAME_ERR
      }
      // (schema或错误类型, json内容)
      (typ, x)
    }).saveAsTextFile("data/jsonTrash/unformated/")
  }

  /**
   * 校验json字段数量，名称，格式是否符合给定schema规范
   * @param value
   * @param schemaName
   * @param schema
   * @return 正常则返回给定schema名称，异常返回异常类型
   */
  def getSchemaType(value: util.Map[String, Object], schemaName:String, schema:StructType):String = {
    if(schema.length != value.size){
      JSON_FIELD_NUM_ERR
    } else {
      var schemaType = schemaName
      schema.fields.foreach(field =>{
        // 判断json值类型是否符合schema定义的
        field.dataType.typeName
        var jsonField = value.get(field.name)
        if(jsonField == null){
          schemaType = JSON_FIELD_NAME_ERR
        } else{
          val jsonValueTypeClass: String = value.get(field.name).getClass.getSimpleName
          val jsonValueType = jsonValueTypeClass.substring(jsonValueTypeClass.lastIndexOf(".") + 1)
          if(field.dataType.typeName.toLowerCase != jsonValueType.toLowerCase){
            schemaType = JSON_FIELD_TYP_ERR
          }
        }
      })
      schemaType
    }
  }

}
