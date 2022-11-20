import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util

object JustTest {
  def main(args: Array[String]): Unit = {
    Array("106.38.176.185"
      ,"106.38.176.117"
      ,"106.38.176.118"
      ,"106.38.176.116"
    ).foreach(ip => println(getPartition(ip)))

  }

  def getPartition(key: Any): Int = {//确定数据进入分区的具体策略
    val keyStr = key.toString
    val keyTag = keyStr.substring(keyStr.length - 1, keyStr.length)
    println(keyTag)
    keyTag.toInt % 4
  }
}
