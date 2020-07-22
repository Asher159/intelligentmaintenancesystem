import java.io.{File, FileWriter}

import com.foxconn.util
import com.foxconn.util.{configUtil, getFilePath}
import com.foxconn.util.readDirectoryAndMatAndIntoHive.getFiles1
import com.foxconn.util.readDirectoryAndMatAndIntoHive.getMatData
import com.foxconn.util.configUtil.parseStringDateFromTs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.KeyValue
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object test02 {
  def main(args: Array[String]): Unit = {
    val kv = new KeyValue(Bytes.toBytes("Rownumber"), Bytes.toBytes("family"), Bytes.toBytes("column"), Bytes.toBytes("value"))
    println(kv)
    println(kv.toString.split("/")(0))
    val sparkConf = new SparkConf().setAppName("kafkaTOHive").setMaster("local[*]")
    sparkConf.set("spark.streaming.kafka.maxRatePerPartition", "25600") //设定对目标topic每个partition每秒钟拉取的数据条数,我的topic IMS有12个分区
    sparkConf.set("spark.streaming.backpressure.enabled", "true") //开启背压机制
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //这里先生成sparkSession ，然后生成sc，在生成ssc,这用就能保证sparkSession、sc、ssc三者共存，不然会报多个sc异常错误
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val pathString = "hdfs://192.168.1.211:8020/tmp/hbase/"
    configUtil.deletHdfsPath(sparkSession, pathString,true)
  }


}

