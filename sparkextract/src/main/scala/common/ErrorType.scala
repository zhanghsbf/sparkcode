package common

object ErrorType extends Serializable {
  // 空串错误
  val JSON_BLANK_ERR:String = "JSON_BLANK_ERR"

  // json格式错误
  val JSON_SYNTAX_ERR:String = "JSON_SYNTAX_ERR"

  // 缺失事件名错误
  val JSON_LACK_EVENT_NAME_ERR:String = "JSON_LACK_EVENT_NAME_ERR"

  // 字段数量不一致错误
  val JSON_FIELD_NUM_ERR:String = "JSON_FIELD_NUM_ERR"

  // 字段名称不一致错误
  val JSON_FIELD_NAME_ERR:String = "JSON_FIELD_NAME_ERR"

  // 字段类型错误
  val JSON_FIELD_TYP_ERR:String = "JSON_FIELD_TYP_ERR"

}

