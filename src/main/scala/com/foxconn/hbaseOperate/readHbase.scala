package com.foxconn.hbaseOperate

import java.util

import com.foxconn.util.configUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, ResultScanner, Scan, Table}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ArrayBuffer

object readHbase {
  val connection: Connection = configUtil.getConnection
  val table: Table = configUtil.getTable
  val configuration: Configuration = configUtil.getConfiguration


  def main(args: Array[String]): Unit = {
    //    val tuples = scanDataFromHTable("inner", "MT1_x_feed_25600")
    val tuples1 = getDataFromRowNumber("MT1_x_in_0.6_0.002_f_2000-13-25-54", 1, 10)
    tuples1.foreach(println)
    configUtil.clearUp()
  }



  // 输入；列簇和列名获取对应所有数据 返回（number,value）
  def scanDataFromHTable(columnFamily: String, column: String): List[(Int, Double)] = {
    //定义scan对象
    val scan = new Scan()
    //添加列簇.列名称
    //    scan.addFamily(columnFamily.getBytes())
    scan.addColumn(columnFamily.getBytes(), column.getBytes())
    //从table中抓取数据来scan
    val scanner = table.getScanner(scan)
    var result = scanner.next()
    val strings = ArrayBuffer[(Int, Double)]()
    //数据不为空时输出数据
    while (result != null) {
      val numStrings = Bytes.toString(result.getRow).split("_")
      val num = numStrings(0).toInt
      val value = Bytes.toString(result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(column))).toDouble
      strings.append((num, value))
      println(s"rowkey:$num,列簇:$columnFamily:$column,value:$value")
      result = scanner.next()
    }
    //通过scan取完数据后，记得要关闭ResultScanner，否则RegionServer可能会出现问题(对应的Server资源无法释放)
    scanner.close()
    strings.toList.sortWith {
      (left, right) => left._1 < right._1 // 升序
    }
  }

  // 输入；rowNumber（排序id_文件名）获取值 返回（number,value）
  def getDataFromRowNumber(filename: String, start: Int, end: Int): List[(Int, Double)] = {
    val gets = new util.ArrayList[Get]()
    for (i <- start - 1 until end) {
      val get = new Get(Bytes.toBytes(i + "_" + filename))
      gets.add(get)
    }
    val arrayresult = table.get(gets)
    val strings = ArrayBuffer[(Int, Double)]()
    //数据不为空时输出数据
    for (result <- arrayresult) {
      val cellScanner = result.cellScanner()
      while (cellScanner.advance) {
        val cell = cellScanner.current
        val rowkey = Bytes.toString(CellUtil.cloneRow(cell))
        val famliy = Bytes.toString(CellUtil.cloneFamily(cell))
        val qualify = Bytes.toString(CellUtil.cloneQualifier(cell))
        val value = Bytes.toString(CellUtil.cloneValue(cell)).toDouble
        val num = rowkey.split("_")(0).toInt
        System.out.println(rowkey + "\t" + famliy + "\t" + qualify + "\t" + value)
        strings.append((num, value))
      }
    }
    strings.toList.sortWith {
      (left, right) => left._1 < right._1 // 升序
    }
  }

}
