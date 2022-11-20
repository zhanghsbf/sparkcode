package skew

import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.spark_project.jetty.server.Connector
import org.spark_project.jetty.util.component.{Container, LifeCycle}

import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util
import java.util.Map.Entry
import scala.util.Random

object SkewSample7 {
  def main(args: Array[String]): Unit = {
    skew4()
  }

  /**
   * 倾斜key单独拿出来跑
   */
  def skew4():Unit = {
    val conf = new SparkConf().setAppName("DataSkewTest01").setMaster("local[4]")
    val spark = new SparkContext(conf)

//    val rawRDD = spark.textFile("/root/data/skewdata.csv")//读取数据源
    val rawRDD = spark.textFile("D:\\BaiduNetdiskDownload\\尚硅谷\\上网DNS日志数据\\part-00000-7dc7257d-dd48-4e7f-9865-d7181c3c4c37-c000.csv")//读取数据源

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

    val reducedRDD_03 = reducedRDD_02.map(kv => {/**第4步：继续减盐，将原本10个分区数的数据恢复到1个*/
      val targetIPWithSalt01 = kv._1
      val clientIPMap = kv._2
      if (targetIPWithSalt01.startsWith("106.38.176.185")){
        val targetIP = targetIPWithSalt01.split("-")(0)
        (targetIP,clientIPMap)//彻底将盐去掉
      }
      else kv
    })


    val finalRDD = reducedRDD_03.reduceByKey(new MyMysqlPartitioner(4), (map1,map2) => { /**合并2个map中的元素，key相同则value值相加*/
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
    })//调整分区数量

    println("asdfsd")
    finalRDD.foreach(x => println(x._1, x._2.size()))
//    targetRDD1.saveAsTextFile("tmp/DataSkew01") //结果数据保存目录

    Thread.sleep(600000)
  }

  /**
   * ip分区器
   * @param keys
   */
  class MyIpPartitioner(partitionNum: Int) extends Partitioner{
    override def numPartitions: Int = partitionNum  //确定总分区数量

    override def getPartition(key: Any): Int = {//确定数据进入分区的具体策略
      val keyStr = key.toString
      val keyTag = keyStr.substring(keyStr.length - 1, keyStr.length)
      keyTag.toInt % partitionNum
    }
  }

  /**
   * keys分区器
   * @param keys
   */
  class MyMapPartitioner(keys:Array[String]) extends Partitioner{
    override def numPartitions: Int = keys.length

    val partitionMap = new util.HashMap[String, Int]()
    var pointer = 0
    keys.foreach(k =>{
      partitionMap.put(k, pointer)
      pointer += 1
      if(pointer == keys.length){
        pointer = 0
      }
    })
    println(partitionMap)

    override def getPartition(key: Any): Int = {
      println(key, "-", partitionMap.get(key))
      partitionMap.get(key)
    }
  }

  /**
   * mysql分区器
   * @param partitions
   */
  class MyMysqlPartitioner(partitions:Int) extends Partitioner{
    val jdbcURL = "jdbc:mysql://192.168.31.95:3306?zyk&useSSL=false"
    val username = "zyktest"
    val password = "123456"

    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      getPartitionByMysql(key)
    }

    /** ddl
CREATE TABLE `pointer` (
  `pointer` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
INSERT INTO pointer VALUES (0);

CREATE TABLE `key_partition_map` (
  `key_string` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `partition_index` tinyint DEFAULT NULL,
  PRIMARY KEY (`key_string`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
     */
    def getPartitionByMysql(key:Any):Int = {
      var partition = 0
      Class.forName("com.mysql.jdbc.Driver")
      val conn: Connection = DriverManager.getConnection(jdbcURL, username, password)

      // 开启事务
      conn.setAutoCommit(false)
      val statement: Statement = conn.createStatement()

      // 用mysql的写锁作为分布式锁, pointer表用于当成锁，和递增的指针， key_partition_map表存储 key和分区的映射
      val lock: ResultSet = statement.executeQuery("select pointer from zyk.pointer for update;")
      lock.next()
      partition = lock.getInt(1)

      val map: ResultSet = statement.executeQuery("select partition_index from zyk.key_partition_map where key_string = '" + key + "';")
      // 如果有结果则取对应映射，如果没有结果，则插入新增映射，并将指针加1
      if(map.next()){
        partition = map.getInt(1)
      } else {
        statement.execute("insert into zyk.key_partition_map values ('" + key + "', " + partition + ");")
        partition += 1
        if(partition == numPartitions) partition = 0
        statement.executeUpdate("update zyk.pointer set pointer = " + partition + ";")
        conn.commit()
      }
      conn.close()
      println(key, "-", partition)
      // 返回最终分区
      partition
    }
  }
}
