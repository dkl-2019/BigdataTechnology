package SparkStreaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    // 1. 初始化环境
    // 在 SparkCore 中的内存，创建 SparkContext 的时候使用
    // 在创建 Streaming Context 的时候也要用到 conf，说明 Spark Streaming 是基于 SparkCore
    // 在执行 master 的时候，不能指定一个线程
    // 因为在 Streaming 运行的时候，需要开一个新的线程来去一直监听数据的获取
    val sparkConf = new SparkConf().setAppName("streaming word count").setMaster("local[6]")
    // StreamingContext 其实就是 Spark Streaming 的入口
    // 相当于 SparkContext 是 SparkCore 的入口一样，它们也都叫 XXContext
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.sparkContext.setLogLevel("WARN")  // 日志设置

    // socketTextStream 这个方法用于创建一个 DStream，监听 Socket 输入，当作文本来处理
    // sparkContext.textFile() 创建一个 rdd，他俩类似，都是创建对应的数据集
    // RDD -> SparkCore       DStream -> Spark Streaming
    // DStream 可以理解为是一个流式的 RDD
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream(
      hostname = "192.168.1.183",
      port = 9999,
      storageLevel = StorageLevel.MEMORY_AND_DISK_SER
    )

    // 2. 数据处理
    //    1. 拆词
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //    2. 转换单词
    val tuples: DStream[(String, Int)] = words.map((_, 1))
    //    3. 词频 reduce
    val counts: DStream[(String, Int)] = tuples.reduceByKey(_ + _)

    // 3. 展示启动
    counts.print()

    ssc.start()

    // main 方法执行完毕后整个程序就会退出，所以需要阻塞主线程
    ssc.awaitTermination()
  }

}
