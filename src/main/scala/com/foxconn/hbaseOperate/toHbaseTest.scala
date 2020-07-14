package com.foxconn.hbaseOperate

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Put, RegionLocator, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 * rowNumber:机床_信号类型_信号序号  列族:信号状态（inner,outer,normal） 列：文件_频率
 * 数据实例：MT1_x_feed_303137  column=inner:MT1_x_in_0.6_0.002_f_10000-13-31-03_25600, timestamp=1593589927746, value=0.273395460081828
 */
object toHbaseTest {
  private val configuration: Configuration = HBaseConfiguration.create() // 将自动读取hbase-site.xml中的配置
  configuration.set(TableOutputFormat.OUTPUT_TABLE, "tbl1")
  configuration.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")   //  java.lang.IllegalArgumentException: Can not create a Path from a null string
  //  private val connection: Connection = ConnectionFactory.createConnection(configuration)
  //  private val table: Table = connection.getTable(TableName.valueOf("tbl1"))
  //  private val admin: Admin = connection.getAdmin
  //  private val locator: RegionLocator = connection.getRegionLocator(TableName.valueOf("tbl1"))


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("toHbase").setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer.max", "128m")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //    val job = Job.getInstance(configuration)
    //    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    //    job.setMapOutputKeyClass(classOf[KeyValue])

    //设置OutputFormat类型
    val job: Job = Job.getInstance(configuration)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    val indataRDD = sparkSession.sparkContext.makeRDD(Array("20180723_02,13", "20180723_03,14", "20180818_03,15"))

    //    HFileOutputFormat2.configureIncrementalLoadMap(job, table)
    //

    //    val rdd = indataRDD.map(x => {
    //      val arr = x.split(",")
    //      val kv = new KeyValue(Bytes.toBytes(arr(0)), "cf1".getBytes, "clict_count".getBytes, arr(1).getBytes)
    //      (new ImmutableBytesWritable(Bytes.toBytes(arr(0))), kv)
    //    })

    val rdd2 = indataRDD.map(x => {
      val arr = x.split(",")
      val put = new Put(Bytes.toBytes(arr(0)))
      put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("clict_count"), arr(1).getBytes)
      (new ImmutableBytesWritable(), put)
    })



    // 保存Hfile to HDFS
    //    rdd.saveAsNewAPIHadoopFile("hdfs://192.168.1.211:8020/tmp/hbase", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], configuration)

    // Bulk写Hfile to HBase
    //    val bulkLoader = new LoadIncrementalHFiles(configuration)
    //    bulkLoader.doBulkLoad(new Path("hdfs://192.168.1.211:8020/tmp/hbase"), admin, table, locator)

    rdd2.saveAsNewAPIHadoopDataset(job.getConfiguration)

    //    table.close()
    //    connection.close()

    sparkSession.close()
  }

}
