package com.foxconn.hbaseOperate

import java.io.File

import com.foxconn.util.configUtil
import com.foxconn.util.readDirectoryAndMatAndIntoHive.{getFiles1, getMatData}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * rowNumber:机床_信号类型_信号序号  列族:信号状态（inner,outer,normal） 列：文件_频率
 * 数据实例：MT1_x_feed_303137  column=inner:MT1_x_in_0.6_0.002_f_10000-13-31-03_25600, timestamp=1593589927746, value=0.273395460081828
 */
object toHbase {
  val configuration: Configuration = HBaseConfiguration.create()
  configuration.set(TableOutputFormat.OUTPUT_TABLE, "intelligentmaintenancesystem")
  val connection: Connection = ConnectionFactory.createConnection(configuration)
  val table: Table = connection.getTable(TableName.valueOf("intelligentmaintenancesystem"))

  //设置OutputFormat类型
  val job: Job = Job.getInstance(configuration)
  job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
  job.setOutputKeyClass(classOf[ImmutableBytesWritable])
  job.setOutputValueClass(classOf[Result])

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("toHbase").setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer.max", "128m")
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    val rootDirectory = configUtil.getValueFromConfig("data.root.directory")
    val dir1 = rootDirectory + File.separator + "后期" + File.separator +
      "训练模型" + File.separator + "模型训练文件" + File.separator + "MT1_x_feed" + File.separator + "原始数据_mat" + File.separator + "x_feed_axis_mat" + File.separator + "inner"
    val str1 = System.getProperty("file.separator")
    var dirs = new Array[String](1)

    if (str1 == "\\") {
      dirs = dir1.split("\\\\")
      val int = dirs.length
      val machine_singnal = dirs(int - 4) // 信号名作为第一个分区
      val status = dirs(int - 1) // 注意此处分割是否正确，
      splitAfter(dir1, sparkSession, machine_singnal, status, int)

    } else {
      dirs = dir1.split("/")
    }

    table.close()
    connection.close()

  }

  // "25600为假设频率"
  def splitAfter(dir: String, sparkSession: SparkSession, machine_singnal: String, status: String, int: Int): Unit = {
    import sparkSession.implicits._
    val files = getFiles1(new File(dir)) // 文件名作为第二个分区
    var filename: String = ""
    files.foreach(x => {
      if (System.getProperty("file.separator") == "\\") {
        filename = x.toString.split("\\\\")(int).replace(".mat", "")
      }
      else {
        filename = x.toString.split("/")(int).replace(".mat", "")
      }

      val value = sparkSession.sparkContext.makeRDD(getMatData(new File(x.toString)), 2)
      val valueRDD = value.zipWithIndex().map {
        //        case (value, num) => (machine_singnal +"_"+ num, status, filename + "_25600", value)
        case (value, num) => (num+"_"+filename, status, machine_singnal+"_25600", value)
      }
      val tuplesArray = valueRDD.collect()
      insertTable(tuplesArray)
      //      val hbaseRDD = valueRDD.map(convert)
      //      hbaseRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
      println("insert successful!!")
    })
  }

  //定义往Hbase插入数据的方法
  def convert(triple: (String, String, String, Double)): (ImmutableBytesWritable, Put) = {
    val put = new Put(Bytes.toBytes(triple._1))
    put.addColumn(Bytes.toBytes(triple._2), Bytes.toBytes(triple._3), Bytes.toBytes(triple._4))
    (new ImmutableBytesWritable, put)
  }

  // ( tuplesArray :: num+"_"+filename, status, machine_singnal+"_25600", value)
  def insertTable(tuplesArray: Array[(String, String, String, Double)]): Unit = {
    import java.util
    val batPut = new util.ArrayList[Put]
    for ((num_filename, status, machine_singnal_fre, value) <- tuplesArray) {
      //准备key 的数据
      val puts = new Put(num_filename.getBytes())
      //添加列簇名,字段名,字段值value
      puts.addColumn(status.getBytes(), machine_singnal_fre.getBytes(), value.toString.getBytes())
      //把数据插入到tbale中
      batPut.add(puts)
      if (batPut.size > 10000000) { // 设置的缓冲区。一个table.put代表一个RPC，如果多次put将造成插入异常缓慢，所以得设置缓冲区
        table.put(batPut)
        batPut.clear()
      }
    }
    table.put(batPut)
    println("insert successful!!")
  }


}
