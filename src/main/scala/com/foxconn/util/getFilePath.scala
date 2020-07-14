package com.foxconn.util

import java.io.File

import scala.collection.mutable.ArrayBuffer

object getFilePath {
  val rootDirectory: String = configUtil.getValueFromConfig("data.root.directory")
  val separator = System.getProperty("file.separator")

  //  dir0, machine_singnal, status, int
  def getPathAndArgs: ArrayBuffer[(String, String, String)] = {
    val arrayBuffer = ArrayBuffer[(String, String, String)]()

    def appendArrayByffer(dir: String, machine_singnal: String, status: String): Unit = {
      arrayBuffer.append((dir, machine_singnal, status))
    }
    // todo I:\后期\训练模型\模型训练文件\MT1_x_feed\原始数据_mat\x_feed_axis_mat

    val dir1 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
      File.separator + "MT1_x_feed" + File.separator + "原始数据_mat" + File.separator + "x_feed_axis_mat" + File.separator + "inner"
    appendArrayByffer(dir1, "MT1_x_feed", "inner")

    //    val dir2 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT1_x_feed" + File.separator + "原始数据_mat" + File.separator + "x_feed_axis_mat" + File.separator + "outer"
    //    appendArrayByffer(dir2, "MT1_x_feed", "outer")

    //    val dir3 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT1_x_feed" + File.separator + "原始数据_mat" + File.separator + "x_feed_axis_mat" + File.separator + "normal"
    //    appendArrayByffer(dir3, "MT1_x_feed", "normal")

    //   todo I:\后期\训练模型\模型训练文件\MT2_micphone\原始数据_mat\MT2_micphone_data

    //    val dir4 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_micphone" + File.separator + "原始数据_mat" + File.separator + "MT2_micphone_data" + File.separator + "inner_0.6_0.04"
    //    appendArrayByffer(dir4, "MT2_micphone", "inner")

    //    val dir5 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_micphone" + File.separator + "原始数据_mat" + File.separator + "MT2_micphone_data" + File.separator + "normal_slightG"
    //    appendArrayByffer(dir5, "MT2_micphone", "normal")

    //    val dir6 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_micphone" + File.separator + "原始数据_mat" + File.separator + "MT2_micphone_data" + File.separator + "outer-0.6-0.04_criticalG"
    //    appendArrayByffer(dir6, "MT2_micphone", "outer")

    // todo  I:\后期\训练模型\模型训练文件\MT2_spindle_z\原始数据_mat\MT2_data\

    //    val dir7 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_spindle_z" + File.separator + "原始数据_mat" + File.separator + "MT2_data" + File.separator + "inner_0.6_0.04"
    //    appendArrayByffer(dir7, "MT2_spindle_z", "inner")
    //
    //    val dir8 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_spindle_z" + File.separator + "原始数据_mat" + File.separator + "MT2_data" + File.separator + "normal"
    //    appendArrayByffer(dir8, "MT2_spindle_z", "normal")
    //
    //    val dir9 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_spindle_z" + File.separator + "原始数据_mat" + File.separator + "MT2_data" + File.separator + "outer_0.6_0.04_criticalG"
    //    appendArrayByffer(dir9, "MT2_spindle_z", "outer")

    // todo  I:\后期\训练模型\模型训练文件\MT2_x_feed\原始数据_mat\MT2_X_and_y_feed_data\

    //    val dir10 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_x_feed" + File.separator + "原始数据_mat" + File.separator + "MT2_X_and_y_feed_data" + File.separator + "inner_0.6_0.04"
    //    appendArrayByffer(dir10, "MT2_X_and_y_feed_data", "inner")
    //
    //    val dir11 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_x_feed" + File.separator + "原始数据_mat" + File.separator + "MT2_X_and_y_feed_data" + File.separator + "normal"
    //    appendArrayByffer(dir11, "MT2_X_and_y_feed_data", "normal")
    //
    //    val dir12 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT2_x_feed" + File.separator + "原始数据_mat" + File.separator + "MT2_X_and_y_feed_data" + File.separator + "outer-0.6-0.04_criticalG"
    //    appendArrayByffer(dir12, "MT2_X_and_y_feed_data", "outer")

    // todo  I:\后期\训练模型\模型训练文件\MT3_micphone\原始数据_mat\micphone_mat

    //    val dir13 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT3_micphone" + File.separator + "原始数据_mat" + File.separator + "micphone_mat" + File.separator + "inner"
    //    appendArrayByffer(dir13, "MT3_micphone", "inner")
    //
    //    val dir14 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT3_micphone" + File.separator + "原始数据_mat" + File.separator + "micphone_mat" + File.separator + "normal"
    //    appendArrayByffer(dir14, "MT3_micphone", "normal")
    //
    //    val dir15 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT3_micphone" + File.separator + "原始数据_mat" + File.separator + "micphone_mat" + File.separator + "outer"
    //    appendArrayByffer(dir15, "MT3_micphone", "outer")

    //    // todo  I:\后期\训练模型\模型训练文件\MT3_y_feed\原始数据_mat\MT3_y_feed_mat\inner
    //
    //    val dir16 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT3_y_feed" + File.separator + "原始数据_mat" + File.separator + "MT3_y_feed_mat" + File.separator + "inner"
    //    appendArrayByffer(dir16, "MT3_y_feed", "inner")
    //
    //    val dir17 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT3_y_feed" + File.separator + "原始数据_mat" + File.separator + "MT3_y_feed_mat" + File.separator + "normal"
    //    appendArrayByffer(dir17, "MT3_y_feed", "normal")
    //
    //    val dir18 = rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "MT3_y_feed" + File.separator + "原始数据_mat" + File.separator + "MT3_y_feed_mat" + File.separator + "outer"
    //    appendArrayByffer(dir18, "MT3_y_feed", "outer")

    // todo  I:\后期\训练模型\模型训练文件\TG_y_feed\原始数据_mat\TG_y_feed_mat\inner

    //    val dir19= rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "TG_y_feed" + File.separator + "原始数据_mat" + File.separator + "TG_y_feed_mat" + File.separator + "inner"
    //    appendArrayByffer(dir19, "TG_y_feed", "inner")
    //
    //    val dir20= rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "TG_y_feed" + File.separator + "原始数据_mat" + File.separator + "TG_y_feed_mat" + File.separator + "outer"
    //    appendArrayByffer(dir20, "TG_y_feed", "outer")
    //
    //    val dir21= rootDirectory + File.separator + "后期" + File.separator + "训练模型" + File.separator + "模型训练文件" +
    //      File.separator + "TG_y_feed" + File.separator + "原始数据_mat" + File.separator + "TG_y_feed_mat" + File.separator + "normal"
    //    appendArrayByffer(dir21, "TG_y_feed", "normal")


    //     todo Test文件
    //    val dir21 = "G:" + File.separator + "input"
    //    appendArrayByffer(dir21, "MT1_x_feed", "inner")
    arrayBuffer
  }

  def main(args: Array[String]): Unit = {
    val args1 = getPathAndArgs
    args1.foreach(println)
  }

}
