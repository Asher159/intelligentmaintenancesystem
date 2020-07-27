package com.foxconn.realTime

import com.foxconn.util.{MyKafkaUtil, configUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.{Admin, RegionLocator, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}
object kafakToHbase {
  private val table: Table = configUtil.getTable
  private val configuration: Configuration = configUtil.getConfiguration
  private val admin: Admin = configUtil.getAdmin
  private val regionlocator: RegionLocator = configUtil.getRegionLocator

  def main(args: Array[String]): Unit = {
    //    val sparkConf = new SparkConf().setAppName("kafkaTOHive").setMaster("local[*]")
    //    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "7000") //设定对目标topic每个partition每秒钟拉取的数据条数,我的topic IMS有12个分区  // 太多会导致宽带受不起
    //    sparkConf.set("spark.streaming.backpressure.enabled", "true") //开启背压机制
    //    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    //这里先生成sparkSession ，然后生成sc，在生成ssc,这用就能保证sparkSession、sc、ssc三者共存，不然会报多个sc异常错误
    //    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val sparkSession = SparkSession.builder().getOrCreate()
    val sc = sparkSession.sparkContext
    val ssc = new StreamingContext(sc, Seconds(40)) //这里可按频率数设置

    val topic = "IMS"
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)
    //    .transform(rdd=> rdd.sortBy(record=> record.key()+record.value()))
    kafkaDStream.foreachRDD(rdd => {
      val valueRDD = rdd.repartition(4).sortBy(record => record.key() + record.value()).map(record => {
        val key = record.key()
        val message: String = record.value()
        val datas: Array[String] = message.split("\t")
        val kv = new KeyValue(Bytes.toBytes(key + "_" + datas(0)), Bytes.toBytes(datas(1)), Bytes.toBytes(datas(2)), Bytes.toBytes(datas(3)))
        (new ImmutableBytesWritable(Bytes.toBytes(datas(0))), kv)
      })
      //      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // 获取偏移量
      val pathString = "hdfs://192.168.1.211:8020/tmp/hbase/" + System.currentTimeMillis().toString
      val path = new Path(pathString)
      println(valueRDD.take(1).foreach(record => println(record._2)))
      //       删除在hdfs上保存的hfile源文件
      //      configUtil.deletHdfsPath(sparkSession, "hdfs://192.168.1.211:8020/tmp/hbase/", true)
      //       保存Hfile to HDFS
      valueRDD.saveAsNewAPIHadoopFile(pathString, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], configuration)
      // Bulk写Hfile to HBase
      val bulkLoader = new LoadIncrementalHFiles(configuration)
      bulkLoader.doBulkLoad(path, admin, table, regionlocator)

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges // 获取偏移量
      kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges) //提交偏移量
    }
    )
    ssc.start()
    ssc.awaitTermination()
  }

}