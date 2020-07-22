package com.foxconn.realTime

import com.foxconn.util.{configUtil, getFilePath}
import com.foxconn.util.readDirectoryAndMatAndIntoHive.{getFiles1, getMatData}
import java.io._
import java.util.Properties
import scala.math.max
import com.foxconn.util.MyKafkaUtil.DataCallback
import com.foxconn.util.configUtil.parseStringDateFromTs
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}


object fileTOKafka {
  val writer = new FileWriter(new File("writeToFileList.txt"), true) // 用于记录已经操作的文件名称
  writer.write(parseStringDateFromTs(System.currentTimeMillis) + "\n")

  def main(args: Array[String]): Unit = {
    val broker = configUtil.getValueFromConfig("kafka.broker.list")
    val topic = "IMS"
    // 创建Kafka消费者
    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    //    根据配置创建Kafka生产者
    val kafkaProducer = new KafkaProducer[String, String](prop)
    try {
      for ((dir, machine_singnal, status) <- getFilePath.getPathAndArgs) {
        val files = getFiles1(new File(dir))
        val files1 = files.filter(file => file.toString.endsWith(".mat")).sortWith {
          case (left, right) => {
            if (left.getName < right.getName) true
            else false
          }
        }
        // 读取文件
        files1.foreach(file => {
          val doublesArray = getMatData(file)
          val filename = file.toString.split(configUtil.getFileSeparator).filter(path => path.endsWith(".mat"))(0).replace(".mat", "") // 文件名 + 数据序号  作为hbase中Cell的 keyRow
          var startTime = System.currentTimeMillis()
          for (i <- Range(0, doublesArray.length, 1)) {
            val str: StringBuffer = new StringBuffer(filename)
            str.append("\t")
            str.append(status)
            str.append("\t")
            str.append(machine_singnal)
            str.append("_")
            str.append("25600")
            str.append("\t")
            str.append(doublesArray(i))
            kafkaProducer.send(new ProducerRecord[String, String](topic, configUtil.transformInt(i), str.toString), new DataCallback(System.currentTimeMillis(), str.toString))
            if ((i + 1 % 25600) == 0) {  // 控制速度
              Thread.sleep(max(1000 - System.currentTimeMillis() + startTime,0))
              startTime = System.currentTimeMillis()
            }

          }
          writer.write(file.toString + "\t\t" + filename + "\n")
        })
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
    } finally {
      writer.write("\n" + "\n")
      writer.close()
      kafkaProducer.close()
    }
  }
}