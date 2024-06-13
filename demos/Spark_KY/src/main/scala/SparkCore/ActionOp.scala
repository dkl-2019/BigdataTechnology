package SparkCore

import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class ActionOp {

  val conf = new SparkConf().setMaster("local[6]").setAppName("action_op")
  val sc = new SparkContext(conf)

  /*
    需求：最终生成 （结果， price）
    注意：
      1. 函数中传入的 curr参数，并不是 Value，而是一整条数据
      2. reduce 整体上的结果，只有一个
   */
  @Test
  def reduce() = {
    val rdd = sc.parallelize(Seq(("手机", 10.0), ("手机", 15.0), ("电脑", 20.0)))
    val result: (String, Double) = rdd.reduce((curr, agg) => ("总价", curr._2 + agg._2))
    println(result) // reduce的结果是一个元组

  }


  /*
    RDD中自带的foreach算子，注意输出的结果顺序不一定按照原来Seq集合中的顺序
    是因为RDD是并行计算，异步操作
   */
  @Test
  def foreach() = {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4))
    rdd.foreach(item => println(item))
  }


  /*
    count 和 countByKey 的结果相距很远，每次调用 Action 都会生成一个 job，
    job 会运行获取结果，所以在俩个 job中间有大量的 Log，其实就是在启动job

    countByKey的运算结果是一个Map型数据：Map(a -> 2, b -> 1, c -> 1)

    数据倾斜：如果要解决数据倾斜，是不是要先知道谁倾斜，通过countByKey可以查看Key对应的
      数量，从而解决倾斜问题
   */
  @Test
  def count() = {
    val rdd = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("a", 4)))
    println(rdd.count())  // 求出集合中数据的总数
    println(rdd.countByKey()) // 得出 Key
    println(rdd.countByValue())
  }

  /*
    take() 和 takeSample() 都是获取数据，一个是直接获取，一个是采样获取（又放回、无放回）
    first：一般情况下，action 会从所有分区获取数据，相对来说速度比较慢，first 只是获取第一个元素
          所有只会处理第一个分区，所以速度很快，无需处理所有数据
   */
  @Test
  def take() = {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 6))
    rdd.take(3).foreach(item => println(item))  // 返回前N个数据
    println(rdd.first())  // 返回第一个元素
    rdd.takeSample(withReplacement = false, num = 3).foreach(item => println(item))
  }

  // 等等数字运算...   注意对于数字类型的支持，都是Action
  @Test
  def numberic() = {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5, 10, 99, 120, 7))
    println(rdd.max())  // 最大值
    println(rdd.min())  // 最小值
    println(rdd.mean()) // 均值
    println(rdd.sum())  //求和
  }

}
















