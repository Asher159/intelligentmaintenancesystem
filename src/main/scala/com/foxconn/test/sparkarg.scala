package com.foxconn.test


import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object sparkarg {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("sparkarg").setMaster("local[*]")
    //    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "7000") //设定对目标topic每个partition每秒钟拉取的数据条数,我的topic IMS有12个分区  // 太多会导致宽带受不起
    sparkConf.set("spark.streaming.backpressure.enabled", "true") //开启背压机制
    sparkConf.
      setAll(scala.collection.Traversable(("spark.executor.memory", "1g"), ("spark.streaming.kafka.maxRatePerPartition", "8000")))

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //这里先生成sparkSession ，然后生成sc，在生成ssc,这用就能保证sparkSession、sc、ssc三者共存，不然会报多个sc异常错误
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //    val sparkSession = SparkSession.builder().getOrCreate()
    val sc = sparkSession.sparkContext
    val rdd = sc.makeRDD(Array(1, 2, 3, 4))

    sparkSession.conf.getAll.foreach(println)
  }
}
