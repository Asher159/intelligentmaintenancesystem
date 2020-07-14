package com.foxconn.util

import java.io.File

import org.apache.spark.sql.{DataFrame, SparkSession}

import org.ujmp.jmatio.ImportMatrixMAT

object readDirectoryAndMatAndIntoHive {


  def getMatData(file: File): Array[Double] = {
    val testMatrix = ImportMatrixMAT.fromFile(file)
    val dataBaseNumber = testMatrix.getColumnCount.toInt //矩阵的列数
    val RowNumber = testMatrix.getRowCount.toInt //矩阵的列数
    println(dataBaseNumber, RowNumber)
    val spaceAngle = new Array[Double](dataBaseNumber * RowNumber)
    for (i <- Range(0, dataBaseNumber * RowNumber, 1)) {
      spaceAngle(i) = testMatrix.toDoubleMatrix.getAsDouble(i)
    }
    spaceAngle
  }

  // 遍历文件目录，返回所有单个文件的完全路径
  def getFiles1(dir: File): Array[File] = {
    dir.listFiles.filter(_.isFile) ++
      dir.listFiles.filter(_.isDirectory).flatMap(getFiles1)
  }


  // create table intelligentmaintenancesystem(
  //              number int, value String
  //               )
  // partitioned by (machine_singnal String, filename string)
  // row format delimited fields terminated by '\t';

  def insertHive2(sparkSession: SparkSession, dataFrame: DataFrame, partition1: String, partition2: String): Unit = {
    dataFrame.toDF().createOrReplaceTempView("addhive")
    val sql = "insert into  intelligentmaintenancesystem partition(machine_singnal='%s',filename ='%s') select * from addhive"
    val sqlText = String.format(sql, partition1, partition2)
    println(sqlText)
    //    sparkSession.sql(sqlText)
  }

  def main(args: Array[String]): Unit = {
    val sql = "insert into  intelligentmaintenancesystem partition(machine_singnal='%s',filename ='%s') select * from addhive"
    val str1 = "asdas1"
    val str2 = "sadas2"
    println(String.format(sql, str1, str2))
  }
}
