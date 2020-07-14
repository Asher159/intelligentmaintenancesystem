package com.foxconn.util

import java.text.SimpleDateFormat
import java.util.{Date, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, RegionLocator, Table}
import org.apache.spark.rdd.RDD

object configUtil {
  private val configuration: Configuration = HBaseConfiguration.create() // 将自动读取hbase-site.xml中的配置
  private val tableName: TableName = TableName.valueOf("IMSPre-split")
  private val connection: Connection = ConnectionFactory.createConnection(configuration)
  private val table: Table = connection.getTable(tableName)
  private val admin: Admin = connection.getAdmin
  private val regionLocator: RegionLocator = connection.getRegionLocator(tableName)


  def main(args: Array[String]): Unit = {

    //println(getValueFromConfig("jdbc.url"));
    // println(getValueFromCondition("endDate"))
    println(parseStringDateFromTs(formatString = "yyyy-MM-dd"))

  }

  def getConnection: Connection = {
    connection
  }

  def getTable: Table = {
    table
  }

  def getConfiguration: Configuration = {
    configuration
  }

  def getAdmin: Admin = {
    admin
  }

  def getRegionLocator: RegionLocator = {
    regionLocator
  }

  def clearUp(): Unit
  = {
    regionLocator.close()
    admin.close()
    table.close()
    connection.close()
  }

  // f为字符串插值符。函数把1转化为0000001
  def transformInt(part: Int): String = {
    f"$part%7d".replaceAll(" ", "0")
  }

  // 用于打印RDD分区元素个数
  def printPartitionNum(value: RDD[_ <: Any]): Unit = {
    value.mapPartitionsWithIndex {
      (partIdx, iter) => {
        val part_map = scala.collection.mutable.Map[String, Int]()
        while (iter.hasNext) {
          val part_name = "part_" + partIdx;
          if (part_map.contains(part_name)) {
            val ele_cnt = part_map(part_name)
            part_map(part_name) = ele_cnt + 1
          } else {
            part_map(part_name) = 1
          }
          iter.next()
        }
        part_map.iterator
      }
    }.collect().foreach(println)
  }

  // 从一个Array中随机抽取num个元素
  def getRandomNumElement(ints: Array[_ <: Any], num: Int): Array[_ <: Any] = {
    import scala.util.Random
    val arrayBuffer = ints.toBuffer
    val length = ints.length - num
    val rnd = new Random
    for (_ <- 1 to length) {
      val arrayBufferLength = arrayBuffer.length
      arrayBuffer.remove(rnd.nextInt(arrayBufferLength))
    }
    arrayBuffer.toArray
  }


  /**
   * 将时间字符串转换为日期对象
   */
  def formatDateFromString(time: String, formatString: String = "yyyy-MM-dd HH:mm:ss"): Date = {
    val format = new SimpleDateFormat(formatString)
    format.parse(time)
  }

  /**
   * 将时间戳转换为日期字符串
   */
  def parseStringDateFromTs(ts: Long = System.currentTimeMillis, formatString: String = "yyyy-MM-dd HH:mm:ss"): String = {
    val format = new SimpleDateFormat(formatString)
    format.format(new Date(ts))
  }

  /**
   * 判断字符串是否非空，true,非空，false，空
   */
  def isNotEmptyString(s: String): Boolean = {
    s != null && !"".equals(s.trim)
  }

  // 从对应的json文件中读取配置
  def getValueFromCondition(key: String): String = {
    val condition: String = getValueFromProperties("condition", "condition.params.json")
    // 将JSON字符串进行转换
    val jsonObj: JSONObject = JSON.parseObject(condition)
    jsonObj.getString(key)
  }

  //从config文件中读取配置
  def getValueFromConfig(key: String): String = {
    getValueFromProperties("config", key);
  }

  /**
   * 读取配置文件
   */
  def getValueFromProperties(fileName: String, key: String): String = {
    // 使用国际化（i18n）组件读取配置文件，只能读取properties文件
    val bundle = ResourceBundle.getBundle(fileName);
    bundle.getString(key)

  }
}
