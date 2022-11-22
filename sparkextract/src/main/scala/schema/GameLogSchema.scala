package schema

import org.apache.spark.sql.types.{DecimalType, IntegerType, LongType, StringType, StructField, StructType}


object GameLogSchema {
  // 懒加载方便写在最上面
  def schemaMap:Map[String, StructType] = schemas
  lazy val schemas = Map(
    ROLE_RECHARGE -> SCHEMA_ROLE_RECHARGE
    ,USER_LOGIN  -> SCHEMA_USER_LOGIN
    ,TASK_LOG  -> SCHEMA_TASK_LOG
    ,ROLE_LEVEL_UP  -> SCHEMA_ROLE_LEVEL_UP
    ,ROLE_CREATE  -> SCHEMA_ROLE_CREATE
    ,GAME_CREATE  -> SCHEMA_GAME_CREATE
  )

  val ROLE_RECHARGE = "role_recharge"
  val SCHEMA_ROLE_RECHARGE = StructType(
    Array(
        StructField("plat_id", StringType, false)
      , StructField("server_id", IntegerType, false)
      , StructField("channel_id", StringType, false)
      , StructField("user_id", StringType, false)
      , StructField("role_id", StringType, false)
      , StructField("role_name", StringType, false)
      , StructField("event_time", IntegerType, false)
      , StructField("order_id", StringType, false)
      , StructField("acer_count", IntegerType, false)
      , StructField("recharge_amount", DecimalType(10,2), false)
      , StructField("order_status", IntegerType, false)
      , StructField("recharge_purpose", IntegerType, false)
      , StructField("event_name", StringType, false)
    )
  )

  val GAME_CREATE = "game_create"
  val SCHEMA_GAME_CREATE = StructType(
    Array(
      StructField("plat_id" ,StringType,false)
      ,StructField("server_id", LongType,false)
      ,StructField("channel_id", StringType,false)
      ,StructField("user_id", StringType,false)
      ,StructField("client_ip", StringType,false)
      ,StructField("event_time", LongType,false)
      ,StructField("device_brand", StringType,false)
      ,StructField("device_type", StringType,false)
      ,StructField("operating_system", StringType,false)
      ,StructField("operating_version", StringType, false)
      ,StructField("event_name", StringType, false)
    )
  )

  val ROLE_CREATE = "role_create"
  val SCHEMA_ROLE_CREATE = StructType(
    Array(
      StructField("plat_id", StringType,    false)
      ,StructField("server_id", LongType,   false)
      ,StructField("channel_id", StringType,false)
      ,StructField("user_id", StringType,   false)
      ,StructField("role_id", StringType,   false)
      ,StructField("role_name", StringType, false)
      ,StructField("client_ip", StringType, false)
      ,StructField("event_time", LongType,  false)
      ,StructField("job_name", StringType,  false)
      ,StructField("role_sex", LongType,    false)
      ,StructField("event_name", StringType, false)
    )
  )

  val ROLE_LEVEL_UP = "role_level_up"
  val SCHEMA_ROLE_LEVEL_UP = StructType(
    Array(
      StructField("plat_id", StringType,    false)
      ,StructField("server_id", LongType,  false)
      ,StructField("channel_id", StringType, false)
      ,StructField("user_id", StringType,    false)
      ,StructField("role_id", StringType,    false)
      ,StructField("role_name", StringType,  false)
      ,StructField("client_ip", StringType,  false)
      ,StructField("event_time", LongType, false)
      ,StructField("level_befor", LongType,false)
      ,StructField("level_after", LongType, false)
      ,StructField("event_name", StringType, false)
    )
  )

  val TASK_LOG = "task_log"
  val SCHEMA_TASK_LOG = StructType(
    Array(
      StructField("plat_id", StringType,          false)
      ,StructField("server_id", LongType,          false)
      ,StructField("channel_id", StringType,       false)
      ,StructField("user_id", StringType,          false)
      ,StructField("role_id", StringType,          false)
      ,StructField("role_name", StringType,        false)
      ,StructField("event_time", LongType,         false)
      ,StructField("task_type", LongType,          false)
      ,StructField("task_id", LongType,            false)
      ,StructField("cost_time", LongType,          false)
//      ,StructField("op_type", StringType,          false)
      ,StructField("op_type", IntegerType,          false)  // 实际数据结构为int
      ,StructField("level_limit", LongType,        false)
      ,StructField("award_exp", LongType,          false)
      ,StructField("award_monetary", StringType,   false)
      ,StructField("award_item", StringType,       false)
      ,StructField("death_count", LongType,        false)
      ,StructField("award_attribute", StringType,  false)
      ,StructField("event_name", StringType, false)
    )
  )

  val USER_LOGIN = "user_login"
  val SCHEMA_USER_LOGIN = StructType(
    Array(
      StructField("plat_id", StringType,           false)
      ,StructField("server_id", LongType,          false)
      ,StructField("channel_id", StringType,       false)
      ,StructField("user_id", StringType,          false)
      ,StructField("role_id", StringType,          false)
      ,StructField("role_name", StringType,        false)
      ,StructField("client_ip", StringType,        false)
      ,StructField("event_time", LongType,         false)
      ,StructField("op_type", StringType,          false)
      ,StructField("online_time", LongType,        false)
      ,StructField("operating_system", StringType, false)
      ,StructField("operating_version", StringType,false)
      ,StructField("device_brand", StringType,     false)
      ,StructField("device_type", StringType,      false)
      ,StructField("event_name", StringType,        false)
    )
  )

}

