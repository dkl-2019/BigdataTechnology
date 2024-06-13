package SparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class SourceAnalysis {

  @Test
  def wordcount() = {
    // 1. 创建sc对象
    val conf = new SparkConf().setAppName("wordcount_source").setMaster("local[6]")
    val sc = new SparkContext(conf)

    // 2. 创建数据集
    val testRDD = sc.parallelize(Seq("hadoop spark", "hadoop flume", "spark sqoop"))

    // 3. 数据处理
    //    1. 拆词：testRDD.flatMap( item => item.split(" ") )
    val splitRDD = testRDD.flatMap( _.split(" ") )
    //    2. 赋予初始词频：tupleRDD = splitRDD.map( item => (item, 1) )
    val tupleRDD = splitRDD.map( (_, 1) )
    //    3. 聚合统计词频：tupleRDD.reduceByKey( (curr, agg) => curr + agg )
    val reduceRDD = tupleRDD.reduceByKey( _ + _ ) // 俩个_ 代表俩个参数，并不是一个参数使用俩次
    //    4. 将结果转为字符串
    val strRDD = reduceRDD.map( item => s"${item._1}, ${item._2}")

    // 4. 获取结果
    strRDD.collect().foreach( println(_) )
    println(strRDD.toDebugString)
    // 5. 关闭sc
    sc.stop()
  }

  @Test
  def narrowDenpendency() = {
    // 1. 创建sc对象
    val conf = new SparkConf().setAppName("cartesian").setMaster("local[6]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
    val rdd2 = sc.parallelize(Seq("a", "b", "c"))

    // 2. 计算 笛卡尔积
    val resultRDD = rdd1.cartesian(rdd2)
    // 3. 获取结果
    resultRDD.collect().foreach(println(_))

    sc.stop()
  }

}
