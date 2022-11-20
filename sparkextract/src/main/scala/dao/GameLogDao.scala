package dao
import dao.GameLogDao._
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer

import scala.collection.mutable

case class GameLogDao(df:DataFrame) extends BaseDao{
  override var dataSource: mutable.HashMap[String, String] = _
  import common.ApplicationContext.session.implicits._

  def saveByType(): Unit = {
    df.cache()
    saveJdbcTable(df.filter($"event_name" === "role_recharge"), "zyk_role_recharge", role_recharge)
    saveJdbcTable(df.filter($"event_name" === "role_level_up"), "zyk_role_level_up", role_level_up)
    saveJdbcTable(df.filter($"event_name" === "task_log"), "zyk_task_log", task_log)
    saveJdbcTable(df.filter($"event_name" === "role_create"), "zyk_role_create", role_create)
    saveJdbcTable(df.filter($"event_name" === "user_login"), "zyk_user_login", user_login)
    saveJdbcTable(df.filter($"event_name" === "game_create"), "zyk_game_create", game_create)
  }

  def saveHiveByType(partition:String): Unit = {
    df.cache()
    saveHiveTable(df.filter($"event_name" === "role_recharge"), "ds_spark", "zyk_role_recharge", role_recharge, partition)
    saveHiveTable(df.filter($"event_name" === "role_level_up"), "ds_spark", "zyk_role_level_up", role_level_up, partition)
    saveHiveTable(df.filter($"event_name" === "task_log"), "ds_spark", "zyk_task_log", task_log, partition)
    saveHiveTable(df.filter($"event_name" === "role_create"), "ds_spark", "zyk_role_create", role_create, partition)
    saveHiveTable(df.filter($"event_name" === "user_login"), "ds_spark", "zyk_user_login", user_login, partition)
    saveHiveTable(df.filter($"event_name" === "game_create"), "ds_spark", "zyk_game_create", game_create, partition)
  }
}

object GameLogDao {
  val role_recharge = ListBuffer("plat_id",
    "server_id",
    "channel_id",
    "user_id",
    "role_id",
    "role_name",
    "event_time",
    "order_id",
    "acer_count",
    "recharge_amount",
    "order_status",
    "recharge_purpose")

  val role_level_up = ListBuffer("plat_id"
    ,"server_id"
    ,"channel_id"
    ,"user_id"
    ,"role_id"
    ,"role_name"
    ,"client_ip"
    ,"event_time"
    ,"level_befor"
    ,"level_after")

  val task_log = ListBuffer("plat_id"
    ,"server_id"
    ,"channel_id"
    ,"user_id"
    ,"role_id"
    ,"role_name"
    ,"event_time"
    ,"task_type"
    ,"task_id"
    ,"cost_time"
    ,"op_type"
    ,"level_limit"
    ,"award_exp"
    ,"award_monetary"
    ,"award_item"
    ,"death_count"
    ,"award_attribute")

  val role_create = ListBuffer("plat_id"
    ,"server_id"
    ,"channel_id"
    ,"user_id"
    ,"role_id"
    ,"role_name"
    ,"client_ip"
    ,"event_time"
    ,"job_name"
    ,"role_sex")

  val user_login = ListBuffer("plat_id"
    ,"server_id"
    ,"channel_id"
    ,"user_id"
    ,"role_id"
    ,"role_name"
    ,"client_ip"
    ,"event_time"
    ,"op_type"
    ,"online_time"
    ,"operating_system"
    ,"operating_version"
    ,"device_brand"
    ,"device_type")

  val game_create = ListBuffer("plat_id"
    ,"server_id"
    ,"channel_id"
    ,"user_id"
    ,"client_ip"
    ,"event_time"
    ,"device_brand"
    ,"device_type"
    ,"operating_system"
    ,"operating_version")

}
