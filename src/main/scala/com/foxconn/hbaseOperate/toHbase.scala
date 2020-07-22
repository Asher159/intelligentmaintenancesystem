package com.foxconn.hbaseOperate

import java.io.{File, FileWriter}

import com.foxconn.util
import com.foxconn.util.configUtil.parseStringDateFromTs
import com.foxconn.util.{configUtil, getFilePath}
import com.foxconn.util.readDirectoryAndMatAndIntoHive.{getFiles1, getMatData}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{KeyValue, TableName}
import org.apache.hadoop.hbase.client.{Admin, Connection, Put, RegionLocator, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * rowNumber:机床_信号类型_信号序号  列族:信号状态（inner,outer,normal） 列：文件_频率
 * 数据实例：MT1_x_feed_303137  column=inner:MT1_x_in_0.6_0.002_f_10000-13-31-03_25600, timestamp=1593589927746, value=0.273395460081828
 *
 * 提供了三种写入hbase的方法，传统方式、spark自带的RDD模式、Hfile load 方式，hfile写入的速度最块，效率最高，其他两种方式差别不大。
 */

object toHbase {
  private val connection: Connection = configUtil.getConnection
  private val table: Table = configUtil.getTable
  private val configuration: Configuration = configUtil.getConfiguration
  private val admin: Admin = configUtil.getAdmin
  private val regionlocator: RegionLocator = configUtil.getRegionLocator


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("toHbase").setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer.max", "128m")
    /**
     * 在使用splitAfterRDD方法时，只用到了configuration ,所以configuration里面要设置好table，
     * 并且设置好mapreduce.output.fileoutputformat.outputdir，否则报java.lang.IllegalArgumentException: Can not create a Path from a null string
     */
    configuration.set(TableOutputFormat.OUTPUT_TABLE, "IMSPre-split")
    configuration.set("mapreduce.output.fileoutputformat.outputdir", "/tmp")

    val writer = new FileWriter(new File("writeToFileList.txt"), true) // 用于记录已经操作的文件名称
    writer.write(parseStringDateFromTs(System.currentTimeMillis) + "\n")

    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    for (dir0 <- getFilePath.getPathAndArgs) { // 读取文件目录列表，遍历操作
      splitAfter(dir0._1, sparkSession, dir0._2, dir0._3, writer) //动作函数
    }

    writer.write("\n" + "\n")
    writer.close()
    configUtil.clearUp()

    sparkSession.close()
  }


  // "25600为假设频率"
  def splitAfter(dir: String, sparkSession: SparkSession, machine_singnal: String, status: String, writer: FileWriter): Unit = {
    import sparkSession.implicits._
    val files = getFiles1(new File(dir))
    // 过滤掉其中的非mat文件  并随机选取对应个数的文件数进行操作 当 num 大于 文件个数即对所有文件操作
    val filterFiles = util.configUtil.getRandomNumElement(files.filter(file => file.toString.endsWith(".mat")), 100) // 过滤掉其中的非mat文件
    filterFiles.foreach(x => {
      val  filename = x.toString.split(configUtil.getFileSeparator).filter(path => path.endsWith(".mat"))(0).replace(".mat", "")
      val value = sparkSession.sparkContext.makeRDD(getMatData(new File(x.toString)), 4) // 分区避免ooM
      val valueRDD = value.zipWithIndex().map {
        case (value, num) =>
          val withZeroNum = configUtil.transformInt(num.toInt)
          (withZeroNum + "_" + filename, status, machine_singnal + "_25600", value)
      }
      valueRDD.foreachPartition(datas => {
        import java.util
        val batPut = new util.ArrayList[Put]
        for ((num_filename, status, machine_singnal_fre, value) <- datas) {
          //准备key 的数据
          val puts = new Put(num_filename.getBytes())
          //添加列簇名,字段名,字段值value
          puts.addColumn(status.getBytes(), machine_singnal_fre.getBytes(), value.toString.getBytes())
          //把数据插入到tbale中
          batPut.add(puts)
          if (batPut.size > 25000) { // 设置的缓冲区。一个table.put代表一个RPC，如果多次put将造成插入异常缓慢，所以得设置缓冲区 但一次插入速度太快会出现丢数据或者OOM
            table.put(batPut)
            //            println(batPut.size())
            batPut.clear()
          }
        }
        table.put(batPut)
        //        println(batPut.size())
        batPut.clear()
      })
      writer.write(x.toString + "\t\t" + filename + "\n")
    })
  }

  // "25600为假设频率"
  def splitAfterHfile(dir: String, sparkSession: SparkSession, machine_singnal: String, status: String, writer: FileWriter): Unit = {
    import sparkSession.implicits._
    val files = getFiles1(new File(dir))
    // 过滤掉其中的非mat文件  并随机选取对应个数的文件数进行操作 当 num 大于 文件个数即对所有文件操作
    val filterFiles = util.configUtil.getRandomNumElement(files.filter(file => file.toString.endsWith(".mat")), 100)
    //    println(filterFiles+ "dasd")
    var filename: String = ""
    filterFiles.foreach(x => {
      if (System.getProperty("file.separator") == "\\") { // 判断操作系统
        filename = x.toString.split("\\\\").filter(path => path.endsWith(".mat"))(0).replace(".mat", "") // 文件名 + 数据序号  作为hbase中Cell的 keyRow
      }
      else {
        filename = x.toString.split("/").filter(path => path.endsWith(".mat"))(0).replace(".mat", "")
      }
      val value = sparkSession.sparkContext.makeRDD(getMatData(new File(x.toString)), 4) // 分区避免ooM

      val valueRDD = value.zipWithIndex().map {
        case (value, num) =>
          val withZeroNum = configUtil.transformInt(num.toInt)
          val kv = new KeyValue(Bytes.toBytes(withZeroNum + "_" + filename), Bytes.toBytes(status), Bytes.toBytes(machine_singnal + "_25600"), Bytes.toBytes(value.toString))
          (new ImmutableBytesWritable(Bytes.toBytes(withZeroNum + "_" + filename)), kv)
      }

      val job = Job.getInstance(configuration)
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setMapOutputKeyClass(classOf[KeyValue])
      HFileOutputFormat2.configureIncrementalLoadMap(job, table)

      // 保存Hfile to HDFS
      val path = new Path("hdfs://192.168.1.211:8020/tmp/hbase/" + filename)
      valueRDD.saveAsNewAPIHadoopFile("hdfs://192.168.1.211:8020/tmp/hbase/" + filename, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], configuration)
      // Bulk写Hfile to HBase
      val bulkLoader = new LoadIncrementalHFiles(configuration)
      bulkLoader.doBulkLoad(path, admin, table, regionlocator)
      writer.write(x.toString + "\t\t" + filename + "\n")
      // 删除在hdfs上保存的hfile源文件
      val hadoopConf = sparkSession.sparkContext.hadoopConfiguration
      val hdfs = path.getFileSystem(hadoopConf)
      if (hdfs.exists(path)) {
        hdfs.delete(path, true) //容许递归删除
      }
    })
  }

  // "25600为假设频率"
  def splitAfterRDD(dir: String, sparkSession: SparkSession, machine_singnal: String, status: String, writer: FileWriter): Unit = {
    import sparkSession.implicits._
    val files = getFiles1(new File(dir))
    // 过滤掉其中的非mat文件  并随机选取对应个数的文件数进行操作 当 num 大于 文件个数即对所有文件操作
    val filterFiles = util.configUtil.getRandomNumElement(files.filter(file => file.toString.endsWith(".mat")), 100)
    //    println(filterFiles+ "dasd")
    var filename: String = ""
    filterFiles.foreach(x => {
      if (System.getProperty("file.separator") == "\\") { // 判断操作系统
        filename = x.toString.split("\\\\").filter(path => path.endsWith(".mat"))(0).replace(".mat", "") // 文件名 + 数据序号  作为hbase中Cell的 keyRow
      }
      else {
        filename = x.toString.split("/").filter(path => path.endsWith(".mat"))(0).replace(".mat", "")
      }
      val value = sparkSession.sparkContext.makeRDD(getMatData(new File(x.toString)), 4) // 分区避免ooM

      //设置OutputFormat类型
      val job: Job = Job.getInstance(configuration)
      job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      job.setOutputKeyClass(classOf[ImmutableBytesWritable])
      job.setOutputValueClass(classOf[Result])

      val valueRDD = value.zipWithIndex().map {
        case (value, num) =>
          val withZeroNum = configUtil.transformInt(num.toInt)
          val put = new Put(Bytes.toBytes(withZeroNum + "_" + filename))
          put.addColumn(Bytes.toBytes(status), Bytes.toBytes(machine_singnal + "_25600"), Bytes.toBytes(value.toString))
          (new ImmutableBytesWritable(), put)
      }

      valueRDD.saveAsNewAPIHadoopDataset(job.getConfiguration)
      writer.write(x.toString + "\t\t" + filename + "\n")

    })
  }

}
