package skew

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import java.util
import java.util.Map.Entry
import java.util.Set
import scala.util.Random

object SkewSample3 {
  def main(args: Array[String]): Unit = {
    skew3()
  }

  /**
   * 倾斜key单独拿出来跑
   */
  def skew3():Unit = {
    val conf = new SparkConf().setAppName("DataSkewTest01")
    val spark = new SparkContext(conf)

    val rawRDD = spark.textFile("/root/data/skewdata.csv")//读取数据源

    /**筛选满足需要的数据，已到达数据倾斜的目的*/
    val filteredRDD = rawRDD.filter(line => {
      val array = line.split(",")
      val target_ip = array(3)
      target_ip.equals("106.38.176.185") || target_ip.equals("106.38.176.117") || target_ip.equals("106.38.176.118") || target_ip.equals("106.38.176.116")
    })

    filteredRDD.cache()
    val normalKeyRDD = filteredRDD.filter(line => {
      val array = line.split(",")
      val target_ip = array(3)
      target_ip.equals("106.38.176.117") || target_ip.equals("106.38.176.118") || target_ip.equals("106.38.176.116")
    })

    val skewKeyRDD = filteredRDD.filter(line => {
      val array = line.split(",")
      val target_ip = array(3)
      target_ip.equals("106.38.176.185")
    })

    /**根据目的ip进行汇总，将访问同一个目的ip的所有客户端ip进行汇总*/
    val reducedRDD1 = normalKeyRDD.map(line => {
      val array = line.split(",")
      val target_ip = array(3)
      val client_ip = array(0)
      val index = client_ip.lastIndexOf(".")
      val subClientIP = client_ip.substring(0, index) //为了让后续聚合后的value数据量尽可能的少，只取ip的前段部分
      (target_ip,Array(subClientIP))
    }).reduceByKey(_++_,3)//将Array中的元素进行合并，然后将分区调整为已知的4个

    //reducedRDD.foreach(x => println(x._1, x._2.length))  //查看倾斜key

    /**将访问同一个目的ip的客户端，再次根据客户端ip进行进一步统计*/
    val targetRDD1 = reducedRDD1.map(kv => {
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


    /**根据目的ip进行汇总，将访问同一个目的ip的所有客户端ip进行汇总*/
    val reducedRDD2 = skewKeyRDD.map(line => {
      val array = line.split(",")
      val target_ip = array(3)
      val client_ip = array(0)
      val index = client_ip.lastIndexOf(".")
      val subClientIP = client_ip.substring(0, index) //为了让后续聚合后的value数据量尽可能的少，只取ip的前段部分
      (target_ip,Array(subClientIP))
    }).reduceByKey(new MySkewPartitioner(200), _++_)// 将数据随机分配到200个分区中

    //reducedRDD.foreach(x => println(x._1, x._2.length))  //查看倾斜key

    /**将访问同一个目的ip的客户端，再次根据客户端ip进行进一步统计*/
    val targetRDD2 = reducedRDD2.map(kv => {
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

    // 合并结果
    val targetRDD3 = targetRDD2.reduceByKey((v1, v2) => {
      val newMap = new util.HashMap[String,Int]()
      newMap.putAll(v1)
      val value: util.Iterator[Entry[String, Int]] = v2.entrySet().iterator()
      while(value.hasNext){
        val value1: Entry[String, Int] = value.next()
        if (newMap.containsKey(value1.getKey)) {
          val sum = newMap.get(value1.getKey) + value1.getValue
          newMap.put(value1.getKey,sum)
        } else {
          newMap.put(value1.getKey, value1.getValue)
        }
      }
      newMap
    })

    targetRDD1.foreach(x => println(x._1, x._2.size()))
    targetRDD3.foreach(x => println(x._1, x._2.size()))
//    targetRDD1.saveAsTextFile("tmp/DataSkew01") //结果数据保存目录

    Thread.sleep(600000)
  }

  class MySkewPartitioner(partitions: Int) extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      Random.nextInt(partitions)
    }
  }

}
