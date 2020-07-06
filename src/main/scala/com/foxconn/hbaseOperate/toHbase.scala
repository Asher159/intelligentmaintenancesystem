package com.foxconn.hbaseOperate

import java.io.{File, FileWriter}

import com.foxconn.util
import com.foxconn.util.configUtil.parseStringDateFromTs
import com.foxconn.util.{configUtil, getFilePath}
import com.foxconn.util.readDirectoryAndMatAndIntoHive.{getFiles1, getMatData}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * rowNumber:机床_信号类型_信号序号  列族:信号状态（inner,outer,normal） 列：文件_频率
 * 数据实例：MT1_x_feed_303137  column=inner:MT1_x_in_0.6_0.002_f_10000-13-31-03_25600, timestamp=1593589927746, value=0.273395460081828
 */
object toHbase {
  val connection: Connection = configUtil.getConnection
  val table: Table = configUtil.getTable
  val configuration: Configuration = configUtil.getConfiguration


  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("toHbase").setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer.max", "128m")

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
    val filterFiles = files.filter(file => file.toString.endsWith(".mat")) // 过滤掉其中的非mat文件
    var filename: String = ""
    util.configUtil.getRandomNumElement(filterFiles, 20).foreach(x => { // 从满足条件的所有文件中随机选取对应个数的文件数进行操作 当 num 大于 文件个数即对所有文件操作
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
        println("插入成功")
        batPut.clear()
      })
      println(x.toString + "插入成功")
      writer.write(x.toString + "\t\t" + filename + "\n")
    })
  }

}
