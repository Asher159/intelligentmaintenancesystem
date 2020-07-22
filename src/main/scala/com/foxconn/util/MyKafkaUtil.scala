package com.foxconn.util

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}


object MyKafkaUtil {

  val broker_list: String = configUtil.getValueFromConfig("kafka.broker.list")

  // kafka消费者配置
  val kafkaParam = Map(
    "bootstrap.servers" -> broker_list, //用于初始化链接到集群的地址
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    //用于标识这个消费者属于哪个消费团体
    "group.id" -> "IMS10",
    //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
    //可以使用这个配置，latest自动重置偏移量为最新的偏移量
    "auto.offset.reset" -> "earliest",
    //如果是true，则这个消费者的偏移量会在后台自动提交,但是kafka宕机容易丢失数据
    //如果是false，会需要手动维护kafka偏移量
    "max.poll.records" -> "10",
    "enable.auto.commit" -> (false: java.lang.Boolean) // true
  )

  // 创建DStream，返回接收到的输入数据
  // LocationStrategies：根据给定的主题和集群地址创建consumer
  // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
  // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
  // ConsumerStrategies.Subscribe：订阅一系列主题

  def getKafkaStream(topic: String, ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val dStream = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Array(topic), kafkaParam))
    dStream
  }


  import org.apache.kafka.clients.producer.Callback
  import org.apache.kafka.clients.producer.RecordMetadata
  import org.slf4j.Logger
  import org.slf4j.LoggerFactory


  /**
   * kafka回调函数
   */
  object DataCallback {
    private val logger = LoggerFactory.getLogger(classOf[DataCallback])
  }

  class DataCallback(val startTime: Long, val message: String) extends Callback {
    /**
     * 生产者成功发送消息，收到kafka服务端发来的ACK确认消息后，会调用此回调函数
     *
     * @param recordMetadata 生产者发送的消息的元数据，如果发送过程中出现异常，此参数为null
     * @param e              发送过程中出现的异常，如果发送成功，则此参数为null
     */
    override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
      if (recordMetadata != null) {
        val endTime = System.currentTimeMillis - startTime
//        DataCallback.logger.info("callback success, message(" + message + ") send to partition(" + recordMetadata.partition + ")," + "offset(" + recordMetadata.offset + ") in" + endTime)
        println("callback success, message(" + message + ") send to partition(" + recordMetadata.partition + ")," + "offset(" + recordMetadata.offset + ") in  " + endTime)
      }
      else e.printStackTrace()
    }
  }

}

