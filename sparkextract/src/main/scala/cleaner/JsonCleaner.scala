package cleaner

import com.alibaba.fastjson2.{JSON, JSONObject}
import common.ErrorType._
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

import java.util


class JsonCleaner{

}

object JsonCleaner{
  private val logger: Logger = LoggerFactory.getLogger(classOf[JsonCleaner])

  /**
   * 异常json返回异常类型，正常json返回对应的schema名称
   * @param content
   * @param keyField
   * @param schemaMap
   * @return
   */
  def clean(content:String, keyField:String, schemaMap:Map[String, StructType]):String = {
    var contentTyp:String = ""
    var structureValue:JSONObject = null
    // 1 如果是空串或者格式非法
    try{
      if(content == "") {
        contentTyp = JSON_BLANK_ERR
      } else {
        structureValue = JSON.parseObject(content)
      }
    } catch{
      case e:Exception => contentTyp = JSON_SYNTAX_ERR;
    }
    // 判断json是否符合对应schema规范
    if(structureValue != null){
      structureValue.get(keyField) match {
        case "" => contentTyp = JSON_LACK_EVENT_NAME_ERR
        case evt:String => contentTyp = checkSchema(structureValue, evt, schemaMap.get(evt))
        case _ => contentTyp = JSON_LACK_EVENT_NAME_ERR
      }
    }
    else contentTyp = JSON_SYNTAX_ERR

    contentTyp
  }


  /**
   * 校验json字段数量，名称，格式是否符合给定schema规范
   * @param value
   * @param schemaName
   * @param schema
   * @return 正常则返回给定schema名称，异常返回异常类型
   */
  def checkSchema(value: util.Map[String, Object], schemaName:String, schema:Option[StructType]):String = {
    schema match{
      case None => JSON_LACK_SCHEMA_ERR
      case Some(schemaStruct) =>
        if(schemaStruct.length != value.size){
          JSON_FIELD_NUM_ERR
        } else {
          var schemaType = schemaName
          schemaStruct.fields.foreach(field =>{
            // 判断json值类型是否符合schema定义的
            val jsonField = value.get(field.name)
            if(jsonField == null){
              schemaType = JSON_FIELD_NAME_ERR
              // logger.debug(s"清洗出异常数据! 错误类型：JSON_FIELD_NAME_ERR 字段名称：$field.name 模式名：$schemaName 实际数据: $value")
            } else{
              val jsonValueTypeClass: String = jsonField.getClass.getSimpleName
              val jsonValueType = jsonValueTypeClass.substring(jsonValueTypeClass.lastIndexOf(".") + 1)
              if(!typeEqual(jsonValueType, field)){
                schemaType = JSON_FIELD_TYP_ERR
                // logger.debug(s"清洗出异常数据! 错误类型：JSON_FIELD_TYP_ERR 字段名称：$field.name 模式名：$schemaName 实际数据: $value")
              }
            }
          })
          schemaType
        }
    }
  }

  /**
   * json解析类型是否匹配shcema定义
   * @param jsonValueType
   * @param field
   * @return 匹配返回true
   */
  def typeEqual(jsonValueType:String, field:StructField):Boolean = {
    val jsonLowerString = jsonValueType.toLowerCase
    val structLowerString = field.dataType.typeName.toLowerCase

    // 特殊匹配
    val map:util.Map[String,String] = new util.HashMap()
    map.put("integer", "long")
    map.put("bigdecimal", "decimal")

    structLowerString == jsonLowerString ||
      structLowerString.contains(map.getOrDefault(jsonLowerString,"NoneSuchType"))
  }
}
