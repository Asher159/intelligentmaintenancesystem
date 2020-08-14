package com.foxconn.realTime

import java.io.{BufferedReader, IOException, InputStream, InputStreamReader}
import java.net.{ServerSocket, Socket}
import java.util.Properties

import com.foxconn.util.MyKafkaUtil.DataCallback
import com.foxconn.util.configUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

object TcpRecv {
  def main(args: Array[String]): Unit = {
    new Thread(new DD).start() //先开启接收端的线程
  }
}

//接收端的代码
class DD extends Runnable {
  val ss = new ServerSocket(configUtil.getValueFromConfig("tcp.send.port").toInt) //创建一个serversocket其端口与发送端的端口是一样的
  val s: Socket = ss.accept //侦听并接受到此套接字的连接，返回一个socket对象
  val is: InputStream = s.getInputStream //获取到输入流
  val isReader = new InputStreamReader(is)
  val reader: BufferedReader = new BufferedReader(isReader)
  val broker: String = configUtil.getValueFromConfig("kafka.broker.list")
  val topic: String = configUtil.getValueFromConfig("tcp.recv.tokafka.topic")
  // 创建Kafka消费者
  val prop = new Properties()
  // 添加配置
  prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
  prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  //    根据配置创建Kafka生产者
  val kafkaProducer = new KafkaProducer[String, String](prop)
  val name: String = configUtil.getValueFromConfig("tcp.recv.tokafka.sign.name")
  val status: String = configUtil.getValueFromConfig("tcp.recv.tokafka.sign.status")
  val machine_singnal: String = configUtil.getValueFromConfig("tcp.recv.tokafka.sign.machine_singnal")

  override def run(): Unit = {
    try {
      //      val buf = new Array[Byte](256) //接收收到的数据
      //      var line = is.read(buf)
      var line = reader.readLine()
      var i = 0
      while (line != null) {
        val str: StringBuffer = new StringBuffer(name)
        str.append("\t")
        str.append(status)
        str.append("\t")
        str.append(machine_singnal)
        str.append("_")
        str.append(configUtil.getValueFromConfig("tcp.recv.tokafka.sign.frequency"))
        str.append("\t")
        str.append(line)
        line = reader.readLine()
        println(str.toString + "\t" + i)
//        kafkaProducer.send(new ProducerRecord[String, String](topic, configUtil.transformInt(i), str.toString), new DataCallback(System.currentTimeMillis(), str.toString))
        i = i + 1
      }
      println(i)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      kafkaProducer.close()
    }
  }
}