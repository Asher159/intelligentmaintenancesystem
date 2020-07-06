package com.foxconn.util

object sampleClass {

}

case class numberValue(number: String, Value: String)


class CustomerPartitioner(numParts: Int) extends org.apache.spark.Partitioner {

  //覆盖分区数
  override def numPartitions: Int = numParts

  //覆盖分区号获取函数
  override def getPartition(key: Any): Int = {
    (key.toString.toLong % numParts).toInt
  }
}
