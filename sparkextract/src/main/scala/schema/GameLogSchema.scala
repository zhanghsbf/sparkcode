package schema

import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer

object GameLogSchema {
  val ROLE_RECHARGE = "role_recharge"
  val role_recharge = StructType(
    Array(
        StructField("plat_id", StringType, false)
      , StructField("server_id", StringType, false)
      , StructField("channel_id", StringType, false)
      , StructField("user_id", StringType, false)
      , StructField("role_id", StringType, false)
      , StructField("role_name", StringType, false)
      , StructField("event_time", StringType, false)
      , StructField("order_id", StringType, false)
      , StructField("acer_count", StringType, false)
      , StructField("recharge_amount", StringType, false)
      , StructField("order_status", StringType, false)
      , StructField("recharge_purpose", StringType, false)
      , StructField("event_name", StringType, false)
    )
  )
}
