package cn.itcast.spark.start

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object wordcountTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("wordcountTest")
    val sc = new SparkContext(conf)

    val RDD1: RDD[String] = sc.textFile("hdfs:///datas/wordcount.data")
    val RDD2 = RDD1.flatMap(item => item.split(" "))
    RDD2.collect().foreach(item => println(item))
    println("------------------------")
    val RDD3 = RDD2.map(item => (item, 1))
    RDD3.collect().foreach(item => println(item))
    println("------------------------")
    val RDD4 = RDD3.reduceByKey((curr, agg) => curr + agg)
    RDD4.collect().foreach(item => println(item))


    sc.stop()
  }

}
