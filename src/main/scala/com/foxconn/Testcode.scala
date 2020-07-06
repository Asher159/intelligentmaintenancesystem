package com.foxconn

import java.io.File

import com.foxconn.util.CustomerPartitioner
import com.foxconn.util.readDirectoryAndMatAndIntoHive.getMatData
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

object Testcode {

  def main(args: Array[String]): Unit = {

    //创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //创建SparkContext
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val x = "I:\\后期\\训练模型\\模型训练文件\\MT2_micphone\\原始数据_mat\\MT2_micphone_data\\inner_0.6_0.04\\20190422153248_inner_0.6_0.04_al7075_10000rpm_depth0.1_width3_feed1200.mat"


//        val value = sparkSession.sparkContext.makeRDD(getMatData(new File(x.toString)), 3) // 分区避免ooM
    val value = sparkSession.sparkContext.makeRDD(Array(1, 20, 300, 4000, 50000, 600000,7000000,80000000,900000000,1200000000), 3) // 分区避免ooM

    val value1 = value.zipWithIndex().map {
      case (value, num) => {
        val newnum = f"$num%9d".replaceAll(" ", "0")
        (newnum,value)
      }
    }.partitionBy(new CustomerPartitioner(4))

    val value2 = value.map {value3=>{
        val newnum = f"${value3.toInt}%9d".replaceAll(" ", "0")
        (newnum,value3)
      }
    }.partitionBy(new CustomerPartitioner(4))

    value2.collect().foreach(println)
    value1.mapPartitionsWithIndex {
      (partIdx, iter) => {
        var part_map = scala.collection.mutable.Map[String, Int]()
        while (iter.hasNext) {
          var part_name = "part_" + partIdx;
          if (part_map.contains(part_name)) {
            var ele_cnt = part_map(part_name)
            part_map(part_name) = ele_cnt + 1
          } else {
            part_map(part_name) = 1
          }
          iter.next()
        }
        part_map.iterator
      }
    }.collect().foreach(println)

    sparkSession.close()
  }

  def write(args: Array[String]) {
    //获取Spark配置信息并创建与spark的连接
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)

    //创建HBaseConf
    val conf = HBaseConfiguration.create()
    val job: Job = Job.getInstance(conf)

    //设置OutputFormat类型
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])

    //定义往Hbase插入数据的方法
    def convert(triple: (String, String, String)): (ImmutableBytesWritable, Put) = {
      val put = new Put(Bytes.toBytes(triple._1))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(triple._2))
      put.addImmutable(Bytes.toBytes("info"), Bytes.toBytes("price"), Bytes.toBytes(triple._3))
      (new ImmutableBytesWritable, put)
    }

    //创建一个RDD
    val initialRDD = sc.parallelize(List(("1", "apple", "11"), ("2", "banana", "12"), ("3", "pear", "13")))

    //将RDD内容写到HBase
    val localData = initialRDD.map(convert)

    localData.saveAsNewAPIHadoopDataset(job.getConfiguration)
  }


}
