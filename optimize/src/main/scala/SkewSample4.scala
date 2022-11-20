import org.apache.spark.{SparkConf, SparkContext}

import java.util
import java.util.Map.Entry
import scala.util.Random

object SkewSample4 {
  def main(args: Array[String]): Unit = {
    skew4()
  }

  /**
   * 倾斜key单独拿出来跑
   */
  def skew4():Unit = {
    val conf = new SparkConf().setAppName("DataSkewTest04")
    val spark = new SparkContext(conf)

    val rawRDD = spark.textFile("/root/data/skewdata.csv")//读取数据源
//    val rawRDD = spark.textFile("D:\\BaiduNetdiskDownload\\尚硅谷\\上网DNS日志数据\\part-00000-7dc7257d-dd48-4e7f-9865-d7181c3c4c37-c000.csv")//读取数据源

    /**筛选满足需要的数据，已到达数据倾斜的目的*/
    val filteredRDD = rawRDD.filter(line => {
      val array = line.split(",")
      val target_ip = array(3)
      target_ip.equals("106.38.176.185") || target_ip.equals("106.38.176.117") || target_ip.equals("106.38.176.118") || target_ip.equals("106.38.176.116")
    })

    val reducedRDD_01 = filteredRDD.map(line => {/**解决倾斜第一步：加盐操作将原本1个分区的数据扩大到100个分区*/
      val array = line.split(",")
      val target_ip = array(3)
      val client_ip = array(0)
      val index = client_ip.lastIndexOf(".")
      val subClientIP = client_ip.substring(0, index)//为了让后续聚合后的value数据量尽可能的少，只取ip的前段部分
      if (target_ip.equals("106.38.176.185")){/**针对特定倾斜的key进行加盐操作*/
        val saltNum = 99 //将原来的1个key增加到100个key
        val salt = new Random().nextInt(saltNum)
        (target_ip + "-" + salt,Array(subClientIP))
      }
      else (target_ip,Array(subClientIP))
    }).reduceByKey(_++_,103)//将Array中的元素进行合并,并确定分区数量

    val targetRDD_01 = reducedRDD_01.map(kv => {/**第二步：将各个分区中的数据进行初步统计，减少单个分区中value的大小*/
      val map = new util.HashMap[String,Int]()
      val target_ip = kv._1
      val clientIPArray = kv._2
      clientIPArray.foreach(clientIP => {//对clientIP进行统计
        if (map.containsKey(clientIP)) {
          val sum = map.get(clientIP) + 1
          map.put(clientIP,sum)
        }
        else map.put(clientIP,1)
      })
      (target_ip,map)
    })

    val reducedRDD_02 = targetRDD_01.map(kv => {/**第3步：对倾斜的数据进行减盐操作，将分区数从100减到10*/
      val targetIPWithSalt01 = kv._1
      val clientIPMap = kv._2
      if (targetIPWithSalt01.startsWith("106.38.176.185")){
        val targetIP = targetIPWithSalt01.split("-")(0)
        val saltNum = 9 //将原来的100个分区减少到10个分区
        val salt = new Random().nextInt(saltNum)
        (targetIP + "-" + salt,clientIPMap)
      }
      else kv
    }).reduceByKey((map1,map2) => { /**合并2个map中的元素，key相同则value值相加*/
      //将map1和map2中的结果merge到map3中，相同的key，则value相加
      val map3 = new util.HashMap[String,Int](map1)
      val value: util.Iterator[Entry[String, Int]] = map2.entrySet().iterator()
      while(value.hasNext){
        val value1: Entry[String, Int] = value.next()
        if (map3.containsKey(value1.getKey)) {
          val sum = map3.get(value1.getKey) + value1.getValue
          map3.put(value1.getKey,sum)
        } else {
          map3.put(value1.getKey, value1.getValue)
        }
      }
      map3
    },13)//调整分区数量

    val finalRDD = reducedRDD_02.map(kv => {/**第4步：继续减盐，将原本10个分区数的数据恢复到1个*/
      val targetIPWithSalt01 = kv._1
      val clientIPMap = kv._2
      if (targetIPWithSalt01.startsWith("106.38.176.185")){
        val targetIP = targetIPWithSalt01.split("-")(0)
        (targetIP,clientIPMap)//彻底将盐去掉
      }
      else kv
    }).reduceByKey((map1,map2) => { /**合并2个map中的元素，key相同则value值相加*/
      //将map1和map2中的结果merge到map3中，相同的key，则value相加
      val map3 = new util.HashMap[String,Int](map1)
      val value: util.Iterator[Entry[String, Int]] = map2.entrySet().iterator()
      while(value.hasNext){
        val value1: Entry[String, Int] = value.next()
        if (map3.containsKey(value1.getKey)) {
          val sum = map3.get(value1.getKey) + value1.getValue
          map3.put(value1.getKey,sum)
        } else {
          map3.put(value1.getKey, value1.getValue)
        }
      }
      map3
    },4)//调整分区数量

    finalRDD.foreach(x => println(x._1, x._2.size()))
//    targetRDD1.saveAsTextFile("tmp/DataSkew01") //结果数据保存目录

  }
}
