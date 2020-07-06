package com.foxconn

import java.io.File

import com.foxconn.util.readDirectoryAndMatAndIntoHive.{getFiles1, getMatData, insertHive2}
import com.foxconn.util.{configUtil, numberValue}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.ujmp.jmatio.ImportMatrixMAT

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

//import scala.reflect.io.File

object tohive {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafkaTOHive").setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer.max", "128m")
    //enableHiveSupport() 打开hive支持，spark会默认去读取配置文件Hive-site.xml中的配置
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    import sparkSession.implicits._
    val rootDirectory = configUtil.getValueFromConfig("data.root.directory")

    val dir1 = rootDirectory + File.separator + "后期" + File.separator +
      "训练模型" + File.separator + "模型训练文件" + File.separator + "MT1_x_feed" + File.separator + "原始数据_mat" + File.separator + "x_feed_axis_mat"

    val dir2 = rootDirectory + File.separator + "后期" + File.separator +
      "训练模型" + File.separator + "模型训练文件" + File.separator + "MT2_ae_rms" + File.separator + "处理后的训练和测试数据集"

    val dir3 = rootDirectory + File.separator + "后期" + File.separator +
      "训练模型" + File.separator + "模型训练文件" + File.separator + "MT2_micphone" + File.separator + "原始数据_mat" + File.separator + "MT2_micphone_data" + File.separator + "inner_0.6_0.04"

    //    readMatToHive(dir1, sparkSession) // (30,1) 30个文件每个文件一列
    //    readMatToHiveDF(dir2, sparkSession) // (1,2048,30240) 1个文件每个文件2048列，共30240行
    readMatToHiveMergeSmallData(dir3, sparkSession)

    sparkSession.close()

  }

  // 适用于小数据集写入 dir : mat文件的父路径，sparksession: 打开hive支持的spark会话。该函数用于把路径下所有mat文件写入hive
  def readMatToHive(dir: String, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val str1 = System.getProperty("file.separator")
    var dirs = new Array[String](1)
    if (str1 == "\\") {
      dirs = dir.split("\\\\")
      val int = dirs.length
      val machine_singnal = dirs(int - 3)
      val files = getFiles1(new File(dir))

      files.foreach(x => {
        val listBuffer = new ListBuffer[numberValue]()
        val doubles = getMatData(new File(x.toString))
        val filename = x.toString.split("\\\\")(int).replace(".mat", "") // 注意此处分割是否正确，
        for (i <- doubles.indices) {
          val value = numberValue(i.toString, doubles(i).toString)
          listBuffer += value
        }
        val numberValueRdd = sparkSession.sparkContext.makeRDD(listBuffer.toList)
        insertHive2(sparkSession, numberValueRdd.toDF(), machine_singnal, filename)
      })
    } else {
      dirs = dir.split("/")
      val int = dirs.length
      val machine_singnal = dirs(int - 3)
      val files = getFiles1(new File(dir))
      files.foreach(x => {
        val listBuffer = new ListBuffer[numberValue]()
        val doubles = getMatData(new File(x.toString))
        val filename = x.toString.split("/")(int).replace(".mat", "") // 注意此处分割是否正确，
        for (i <- doubles.indices) {
          val value = numberValue(i.toString, doubles(i).toString)
          listBuffer += value
        }
        val numberValueRdd = sparkSession.sparkContext.makeRDD(listBuffer.toList)
        insertHive2(sparkSession, numberValueRdd.toDF(), machine_singnal, filename)
      })
    }
  }

  //适用于大数据集写入，但效率低.dir : mat文件的父路径，sparksession: 打开hive支持的spark会话。该函数用于把路径下所有mat文件写入hive
  def readMatToHiveBig(dir: String, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val str1 = System.getProperty("file.separator")
    var dirs = new Array[String](1)

    def splitAfter(): Unit = {
      val int = dirs.length
      val machine_singnal = dirs(int - 2)
      val files = getFiles1(new File(dir))

      files.foreach(x => {
        val listBuffer = new ListBuffer[numberValue]()
        val testMatrix = ImportMatrixMAT.fromFile(new File(x.toString)).toDoubleMatrix
        val filename = x.toString.split("\\\\")(int).replace(".mat", "") // 注意此处分割是否正确，
        val dataBaseNumber = testMatrix.getColumnCount.toInt //矩阵的列数
        val RowNumber = testMatrix.getRowCount.toInt //矩阵的行数
        println(dataBaseNumber, RowNumber)


        for (i <- Range(0, dataBaseNumber * RowNumber, 1)) {
          val doubles = testMatrix.getAsDouble(i).toString
          val value = numberValue(i.toString, doubles)
          listBuffer += value
          println(i)
          if (i > 99999 && i % 1000000 == 0) {
            val numberValueRdd = sparkSession.sparkContext.makeRDD(listBuffer.toList)
            insertHive2(sparkSession, numberValueRdd.toDF(), machine_singnal, filename)
            listBuffer.clear()
          }
        }

        val numberValueRdd = sparkSession.sparkContext.makeRDD(listBuffer.toList)
        insertHive2(sparkSession, numberValueRdd.toDF(), machine_singnal, filename)
        listBuffer.clear()

      })
    }

    if (str1 == "\\") {
      dirs = dir.split("\\\\")
      splitAfter()
    } else {
      dirs = dir.split("/")
      splitAfter()
    }
  }

  //适用于大数据集写入，效率高.dir : mat文件的父路径，sparksession: 打开hive支持的spark会话。该函数用于把路径下所有mat文件写入hive
  def readMatToHiveDF(dir: String, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val str1 = System.getProperty("file.separator")
    var dirs = new Array[String](1)

    def splitAfter(): Unit = {
      val int = dirs.length
      val machine_singnal = dirs(int - 2)
      val files = getFiles1(new File(dir))
      files.foreach(x => {
        val testMatrix = ImportMatrixMAT.fromFile(new File(x.toString)).toDoubleMatrix
        val filename = x.toString.split("\\\\")(int).replace(".mat", "") // 注意此处分割是否正确，
        val dataBaseNumber = testMatrix.getColumnCount.toInt //矩阵的列数
        val RowNumber = testMatrix.getRowCount.toInt //矩阵的行数
        println(dataBaseNumber, RowNumber)

        val value = sparkSession.sparkContext.makeRDD(testMatrix.toDoubleArray, 10).flatMap(x => x.toSeq) //这里10个分区为了避免oom
        val frameDF = value.zipWithIndex().map {
          case (value, num) => (num, value)
        }.toDF("number", "Value")
        insertHive2(sparkSession, frameDF, machine_singnal, filename)
      })
    }

    if (str1 == "\\") {
      dirs = dir.split("\\\\")
      splitAfter()
    } else {
      dirs = dir.split("/")
      splitAfter()
    }


  }

  // 适用于大量小数据集合并写入  dir : mat文件的父路径，sparksession: 打开hive支持的spark会话。该函数用于把路径下所有mat文件写入hive
  def readMatToHiveMergeSmallData(dir: String, sparkSession: SparkSession): Unit = {
    import sparkSession.implicits._
    val str1 = System.getProperty("file.separator")
    var dirs = new Array[String](1)

    def splitAfter(): Unit = {
      var mergeArray = ArrayBuffer[Double]() //用于装载小数据合并之后的结果
      val int = dirs.length
      val machine_singnal = dirs(int - 2) // 信号名作为第一个分区
      val files = getFiles1(new File(dir))  // 文件名作为第二个分区
      val filename = dirs(int - 1) // 注意此处分割是否正确，
      var i = 0  // 序号的起止符

      def xieru(): Unit = {
        val value = sparkSession.sparkContext.makeRDD(mergeArray, 2) //这里10个分区为了避免oom
        val frameDF = value.zipWithIndex().map {
          case (value, num) => (num + i, value)
        }.toDF("number", "Value")
        i = frameDF.count().toInt
        insertHive2(sparkSession, frameDF, machine_singnal, filename)
      }

      files.foreach(x => {
        print(x.toString)
        val doublesArray = getMatData(new File(x.toString)).toBuffer
        mergeArray ++= doublesArray
//        val testMatrix = ImportMatrixMAT.fromFile(new File(x.toString)).toDoubleMatrix
//        val dataBaseNumber = testMatrix.getColumnCount.toInt //矩阵的列数
//        val RowNumber = testMatrix.getRowCount.toInt //矩阵的行数
//        println(dataBaseNumber, RowNumber)
//        val value2 = sparkSession.sparkContext.makeRDD(testMatrix.toDoubleArray).flatMap(x => x.toSeq)
//        mergeArray ++= value2.collect().toBuffer
        if (mergeArray.length >= 15000000) { // 达到条数写入一次
          xieru()
          mergeArray.clear()
        }
      })
     xieru()
    }

    if (str1 == "\\") {
      dirs = dir.split("\\\\")
      splitAfter()
    } else {
      dirs = dir.split("/")
      splitAfter()
    }


  }

}


