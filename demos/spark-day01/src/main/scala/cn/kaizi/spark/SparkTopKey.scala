package cn.kaizi.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object SparkTopKey {

  def main(args: Array[String]): Unit = {
    // 创建SparkConf对象，设置应用的配置信息，比如应用名称和应用运行模式
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("SparkWordCount")
    // TODO: 构建SparkContext上下文实例对象，读取数据和调度Job执行
    val sc: SparkContext = new SparkContext(sparkConf)
    // 第一步、读取数据
    // 封装到RDD集合，认为列表List
    val inputRDD: RDD[String] = sc.textFile("hdfs://192.168.1.183:9000/datas/wordcount.data")
    // 第二步、处理数据
    // 调用RDD中函数，认为调用列表中的函数
    // a. 每行数据分割为单词
    val wordsRDD = inputRDD.flatMap(line => line.split("\\s+"))
    // b. 转换为二元组，表示每个单词出现一次
    val tuplesRDD: RDD[(String, Int)] = wordsRDD.map(word => (word, 1))
    // c. 按照Key分组聚合
    val wordCountsRDD: RDD[(String, Int)] = tuplesRDD.reduceByKey((tmp, item) => tmp + item)
    // 第三步、输出数据
    wordCountsRDD.foreach(println)
    /*
    (spark,7)
    (hadoop,5)
    (hbase,1)
    (hive,3)
    (mapreduce,1)
    */
    // TODO: 按照词频count降序排序获取前3个单词, 有三种方式
    println("======================== sortByKey =========================")
    // 方式一：按照Key排序sortByKey函数， TODO： 建议使用sortByKey函数
    /*
    def sortByKey(
    ascending: Boolean = true,
    numPartitions: Int = self.partitions.length
    ): RDD[(K, V)]
    */
    wordCountsRDD
      .map(tuple => tuple.swap) //.map(tuple => (tuple._2, tuple._1))
      .sortByKey(ascending = false)
      .take(4)
      .foreach(println)
    println("======================== sortBy =========================")
    // 方式二：sortBy函数, 底层调用sortByKey函数
    /*
    def sortBy[K](
    f: (T) => K, // T 表示RDD集合中数据类型，此处为二元组
    ascending: Boolean = true,
    numPartitions: Int = this.partitions.length
    )
    (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T]
    */
    wordCountsRDD
      .sortBy(tuple => tuple._2, ascending = false)
      .take(4)
      .foreach(println)
    println("======================== top =========================")
    // 方式三：top函数，含义获取最大值，传递排序规则， TODO：慎用
    /*
    def top(num: Int)(implicit ord: Ordering[T]): Array[T]
    */
    wordCountsRDD
      .top(4)(Ordering.by(tuple => tuple._2))
      .foreach(println)
    // 为了测试，线程休眠，查看WEB UI界面
    Thread.sleep(10000000)
    // TODO：应用程序运行接收，关闭资源
    sc.stop()
  }
}
