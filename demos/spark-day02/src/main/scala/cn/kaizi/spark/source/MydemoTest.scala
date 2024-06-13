package cn.kaizi.spark.source

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MydemoTest {

  def main(args: Array[String]): Unit = {
    // 构建SparkContext对象
    val sc: SparkContext = {
      // a. 创建SparkConf对象
      val sparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      // b. 传递SparkConf对象，创建实例
      val context = SparkContext.getOrCreate(sparkConf) //有就获取，没有创建
      // c. 返回实例对象
      context
    }

    // TODO：创建一个本地集合
    val seq = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 0)
    val inputRDD: RDD[Int] = sc.parallelize(seq, numSlices = 2)
    inputRDD.foreach(item => println(item))
    // 应用结束，关闭资源
    sc.stop()
  }

}
