package cn.kaizi.spark.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkParallelizeTest {
  def main(args: Array[String]): Unit = {
    // 创建应用程序入口SparkContext实例对象
    val sc: SparkContext = {
      // 1.a 创建SparkConf对象，设置应用的配置信息
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      // 1.b 传递SparkConf对象，构建Context实例
      new SparkContext(sparkConf)
    }
    sc.setLogLevel("WARN")
    // TODO: 1、Scala中集合Seq序列存储数据
    val linesSeq: Seq[String] = Seq(       
      "hadoop scala hive spark scala sql sql", //
      "hadoop scala spark hdfs hive spark", //
      "spark hdfs spark hdfs scala hive spark" //
    )
    // TODO: 2、并行化集合创建RDD数据
    /*
      def parallelize[T: ClassTag](
        seq: Seq[T],
        numSlices: Int = defaultParallelism
      ): RDD[T]
    */
    val inputRDD: RDD[String] = sc.parallelize(linesSeq, numSlices = 2)
    //val inputRDD: RDD[String] = sc.makeRDD(linesSeq, numSlices = 2)
    // TODO: 3、调用集合RDD中函数处理分析数据
    val resultRDD: RDD[(String, Int)] = inputRDD
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .reduceByKey(_ + _)
    // TODO: 4、保存结果RDD到外部存储系统（HDFS、MySQL、HBase。。。。）
    resultRDD.foreach(println)
    // 应用程序运行结束，关闭资源
    sc.stop()
  }
}
