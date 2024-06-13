package SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class TransformationOp {

  val conf: SparkConf = new SparkConf().setAppName("transformation_op").setMaster("local[6]")
  val sc = new SparkContext(conf)

  /*
    mapPartitions 和 map算子一样，只不过map是针对每一条数据进行转换，mapPartitions针对一整个
    分区的数据进行转换，所以：
      * 1. map的func参数是单条数据，mapPartitions的func参数是一个集合（一个分区所有的数据）
      * 2. map的func返回值也是单条数据，mapPartitions的func返回值是一个集合
   */
  @Test
  def mapPartitions(): Unit = {
    // 1. 数据生成
    // 2. 算子使用
    // 3. 获取结果
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), numSlices = 2)
      .mapPartitions(iter => {
        iter.foreach(item => println(item))
        iter
      })
      .collect()
  }


  @Test
  def mapPartitions2(): Unit = {
    // 1. 数据生成
    // 2. 算子使用
    // 3. 获取结果
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), numSlices = 2)
      .mapPartitions(iter => {
        // 如果想给集合中的数字都乘10，该如何操作？
        // 遍历 iter 其中每一条数据进行转换，转换完成之后，返回 iter
        val result = iter.map(item => item * 10)  //注意这个的map算子并不是RDD中的，而是Scala中的
        result
      })
      .collect()
      .foreach(item => println(item))
  }


  /*
    mapPartitionsWithIndex 和 mapPartitions 的区别是 func 参数中多了一个参数，分区号
   */
  @Test
  def mapPartitionsWithIndex(): Unit = {
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6), numSlices = 2)
      .mapPartitionsWithIndex( (index, iter) => {
        println("index: " + index)
        iter.foreach(item => println(item))
        iter
      } )
      .collect()
  }

  /*
    filter 可以过滤掉数据集中的一部分元素
    filter 中接受的函数，参数是每一个元素，如果这个函数返回true，当前元素就会被加入新数据集，
          如果返回false，当前元素会被过滤掉
   */
  @Test
  def filter() = {
    sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
      .filter(item => item % 2 == 0)  //取偶数
      .collect()
      .foreach(item => println(item))
  }

  /*
    sample 作用：采样，尽可能减少数据集的规律损失
   */
  @Test
  def sample() = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val rdd2 = rdd1.sample(false, 0.6)
    //第一个参数为false代表无放回采样，0.6是采样比例
    val result = rdd2.collect()
    result.foreach(item => println(item))
  }

  /*
    mapValues 也是 map，只不过map作用于整条数据，mapValues作用于 Value
   */
  @Test
  def mapValues() = {
    sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      .mapValues(item => item * 10)
      .collect()
      .foreach(println(_))
  }

  /*
    交集
   */
  @Test
  def intersection() = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.intersection(rdd2)
      .collect()
      .foreach(println(_))
  }

  /*
    并集
   */
  @Test
  def union() = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.union(rdd2)
      .collect()
      .foreach(println(_))
  }

  /*
    差集
   */
  @Test
  def subtract() = {
    val rdd1 = sc.parallelize(Seq(1, 2, 3, 4, 5))
    val rdd2 = sc.parallelize(Seq(3, 4, 5, 6, 7))

    rdd1.subtract(rdd2) //rdd1-rdd2
      .collect()
      .foreach(println(_))
  }

  /*
    只分组，不聚合
    groupByKey 运算结果格式：(Key, (value1, value2))
    reduceByKey 能不能在 Map 端做 Combiner：1.能不能减少IO
    groupByKey 在 Map端做 Combiner 没有意义
   */
  @Test
  def groupByKey() = {
    sc.parallelize(Seq(("a", 1), ("a", 1), ("c", 3)))
      .groupByKey()
      .collect()
      .foreach(println(_))  //只有一个参数打印输出可以简写
  }

  /*
    CombineByKey 算子中接受三个参数：
      转换数据的函数（初始函数，作用于第一条数据，用于开启整个计算），
      在分区上进行聚合，把所有分区的聚合结果聚合为最终结果

   */
  @Test
  def combineByKey() = {
    // 1.准备集合
    val rdd: RDD[(String, Double)] = sc.parallelize(Seq(
      ("铠", 100.0),
      ("耀", 99.0),
      ("镜", 99.0),
      ("镜", 98.0),
      ("铠", 97.0)
    ))
    // 2.算子操作
    //  2.1 createCombiner 转换数据
    //  2.2 mergeValue 分区上的聚合
    //  2.3 mergeCombiners 把分区上的结果再次聚合，生成最终结果
    val combineResult = rdd.combineByKey(
      createCombiner = (curr: Double) => (curr, 1),
      mergeValue = (curr: (Double, Int), nextValue: Double) => (curr._1 + nextValue, curr._2 + 1),
      mergeCombiners = (curr: (Double, Int), agg:(Double, Int)) => (curr._1 + agg._1, curr._2 + agg._2)
    )

    val resultRDD = combineResult.map( item => (item._1, item._2._1 / item._2._2))

    // 3. 输出数据
    resultRDD.collect().foreach(println(_))
  }

  /*
    foldByKey 和 reduceByKey 的区别是可以指定初始值
    foldByKey 和 Scala中的 foldLeft、foldRight 区别是，这个初始值作用于每一个数据
   */
  @Test
  def foldByKey() = {
    sc.parallelize(Seq(("a", 1), ("a", 1), ("b", 1)))
      .foldByKey(10)((curr, agg) => curr + agg)
      .collect()
      .foreach(println(_))
  }

  /*
    aggregateByKey(zeroValue)(seqOp, combOp)
      zeroValue：指定初始值
      seqOp：作用于每个元素，根据初始值，进行计算
      combOp：将 seqOp 处理过的结果进行聚合

    aggregateByKey 比较适合针对每个数据要先处理，后聚合的场景
   */
  @Test
  def aggregateByKey() = {
    val rdd = sc.parallelize(Seq(("手机", 10.0), ("手机", 15.0), ("电脑", 20.0)))
    rdd.aggregateByKey(0.8)((zeroValue, item) => item * zeroValue, (curr, agg) => curr + agg)
      .collect()
      .foreach(println(_))
  }

  @Test
  def join() = {
    val rdd1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 1)))
    val rdd2 = sc.parallelize(Seq(("a", 10), ("a", 11), ("b", 12)))
    rdd1.join(rdd2)
      .collect()
      .foreach(println(_))
  }


  /*
    sortBy 可以用于任何类型数据的RDD，sortByKey 只有 KV 类型数据的RDD中才有
    sortBy 可以按照任何部分顺序来排序，sortByKey 只能按照 Key 来排序
    sortByKey 写发简单，不用编写函数了
   */
  @Test
  def sort() = {
    val rdd1 = sc.parallelize(Seq(2, 4, 1, 5, 1, 8))
    val rdd2 = sc.parallelize(Seq(("a", 1), ("b", 3), ("c", 2)))

    println("-----------------------------")
    rdd1.sortBy(item => item).collect().foreach(println(_))
    println("-----------------------------")
    rdd2.sortBy(item => item._2).collect().foreach(println(_))
    println("-----------------------------")
    rdd2.sortByKey().collect().foreach(println(_))
  }


  /*
    repartition 进行重分区的时候，默认是 shuffle 的
    coalesce 进行重分区的时候，默认是不 shuffle 的，coalesce 默认不能增大分区数
   */
  @Test
  def partitioning() = {
    val rdd = sc.parallelize(Seq(1, 2, 3, 4, 5), 2)

    println(rdd.repartition(5).partitions.size)
    println(rdd.repartition(1).partitions.size)
    println(rdd.coalesce(5, shuffle = true ).partitions.size)
  }

}



