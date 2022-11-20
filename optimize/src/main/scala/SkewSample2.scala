import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import java.util

object SkewSample2 {
  def main(args: Array[String]): Unit = {
    skew2()
  }

  /**
   * 扩大shuflle时分区数量
   */
  def skew2():Unit = {
    val session: SparkSession = SparkSession.builder().appName("DataSkewTest02").getOrCreate()
    val spark: SparkContext = session.sparkContext

    val rawRDD = spark.textFile("/root/data/skewdata.csv")//读取数据源

    /**筛选满足需要的数据，已到达数据倾斜的目的*/
    val filteredRDD = rawRDD.filter(line => {
      val array = line.split(",")
      val target_ip = array(3)
      target_ip.equals("106.38.176.185") || target_ip.equals("106.38.176.117") || target_ip.equals("106.38.176.118") || target_ip.equals("106.38.176.116")
    })


    /**根据目的ip进行汇总，将访问同一个目的ip的所有客户端ip进行汇总*/
    val reducedRDD = filteredRDD.map(line => {
      val array = line.split(",")
      val target_ip = array(3)
      val client_ip = array(0)
      val index = client_ip.lastIndexOf(".")
      val subClientIP = client_ip.substring(0, index) //为了让后续聚合后的value数据量尽可能的少，只取ip的前段部分
      (target_ip,Array(subClientIP))
    }).reduceByKey(_++_,12)//将Array中的元素进行合并，然后将分区调整为已知的4个

    //reducedRDD.foreach(x => println(x._1, x._2.length))  //查看倾斜key

    /**将访问同一个目的ip的客户端，再次根据客户端ip进行进一步统计*/
    val targetRDD = reducedRDD.map(kv => {
      val map = new util.HashMap[String,Int]()
      val target_ip = kv._1
      val clientIPArray = kv._2
      clientIPArray.foreach(clientIP => {
        if (map.containsKey(clientIP)) {
          val sum = map.get(clientIP) + 1
          map.put(clientIP,sum)
        }
        else map.put(clientIP,1)
      })
      (target_ip,map)
    })

    targetRDD.foreach(x => println(x._1, x._2.size()))
//    targetRDD.saveAsTextFile("tmp/DataSkew01") //结果数据保存目录

  }
}
