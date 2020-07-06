import java.io.{File, FileWriter}

import com.foxconn.util
import com.foxconn.util.getFilePath
import com.foxconn.util.readDirectoryAndMatAndIntoHive.getFiles1
import com.foxconn.util.readDirectoryAndMatAndIntoHive.getMatData
import com.foxconn.util.configUtil.parseStringDateFromTs


object test02 {
  def main(args: Array[String]): Unit = {
    //    getRandomNumElement(Array(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), 3).foreach(println)
    //    val writer = new FileWriter(new File("test.txt"), true)
    //    writer.write(parseStringDateFromTs(System.currentTimeMillis)+"\n")
    //    val files = getFiles1(new File("I:\\后期\\训练模型\\模型训练文件\\MT2_x_feed\\原始数据_mat\\MT2_X_and_y_feed_data\\outer-0.6-0.04_criticalG\\outer-0.6-0.04_criticalG_unload_15000rpm-11-55-16.mat"))
    //    files.foreach(println)
    getMatData(new File("I:\\后期\\训练模型\\模型训练文件\\TG_y_feed\\原始数据_mat\\TG_y_feed_mat\\inner_feed_1000.mat"))

    //    for (dir0 <- getFilePath.getPathAndArgs) {
    //      val files = getFiles1(new File(dir0._1)) // 文件名作为第二个分区
    //      util.configUtil.getRandomNumElement(files, 100).foreach {
    //        filesPath => {
    //          writer.write(filesPath.toString+"\n")
    //        }
    //      }
    //    }
    //    writer.write("\n\n")
    //    writer.close()
  }
}

