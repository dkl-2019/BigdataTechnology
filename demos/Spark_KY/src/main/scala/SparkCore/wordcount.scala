package SparkCore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class wordcount {
  def main(args: Array[String]): Unit = {
    //1.创建SparkContext
    val conf = new SparkConf().setAppName("word_count").setMaster("local[6]")
    val sc = new SparkContext(conf)

    //2.加载文件
    val rdd1: RDD[String] = sc.textFile(path="hdfs:///datas/wordcount.data", minPartitions=2)

    //3.处理
    val rdd2: RDD[String] = rdd1.flatMap(item => item.split(" "))
    val rdd3: RDD[(String, Int)] = rdd2.map(item => (item, 1))
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey((curr, agg) => curr + agg)

    //4.得到结果
    val result: Array[(String, Int)] = rdd4.collect()
    result.foreach(item => println(item))
  }

  @Test
  def sparkContext() = {
    val conf = new SparkConf().setAppName("word_count").setMaster("local[6]")
    val sc = new SparkContext(conf)
  }

  //放在方法体外，类加载直接执行
  val conf = new SparkConf().setAppName("word_count").setMaster("local[6]")
  val sc = new SparkContext(conf)


  //从本地集合创建
  @Test
  def rddCreationLocal() = {
    val seq = Seq("Hello1", "hello2", "Hello3")
    val rdd1: RDD[String] = sc.parallelize(seq, 2)  //指定分区数目
    val rdd2: RDD[String] = sc.makeRDD(seq, 2) //底层依旧调用的parallelize

    /*def makeRDD[T](seq : scala.Seq[T], numSlices : scala.Int =
    */

    /*
    parallelize makeRDD俩者的区别：
      parallelize可以不指定分区数
      makeRDD不指定就会调用成别的同名方法，可以看下源码来验证，一般不用
     */
  }

  //从文件创建
//  @Test
//  def rddCreationFiles() = {
//    sc.textFile("hdfs:///datas/wordcount.data", minPartitions = 2)
//    /*
//      1. textFile 传入的是什么？
//        * 传入的是一个 path，读取路径
//        * hdfs://   file://   /.../...(这种方式分为在集群中执行和在本地执行，在集群中是hdfs://，本地则是file://)
//      2. 是否支持分区？
//        * 如果传入的path是 hdfs://... ，分区则由hdfs文件的block决定
//      3. 支持什么平台？
//        * 支持外部数据源
//      minPartitions 参数可以指定分区，指的是最小分区数
//     */
//  }

  //从RDD衍生
  @Test
  def rddCreationFromRDD() = {
    val rdd1: RDD[Int] = sc.parallelize(Seq(1, 2, 3))
    val rdd2: RDD[Int] = rdd1.map(item => item)
    /*
    通过在RDD上进行算子操作，会生成新的RDD，那么新的RDD是原来的RDD吗？
    提一个概念，原地计算？类比一下字符串操作 str.substr 返回的是新的 str
    那么这个叫做非原地计算，那么原来的字符串变了吗？当然没变？同理RDD不可变！！！
     */
  }

  @Test
  def mapTest() = {
    // 1. 创建RDD
    val rdd1 = sc.parallelize(Seq(1, 2, 3))
    // 2. 执行 map 操作
    val rdd2 = rdd1.map(item => item * 10)
    // 3. 得到结果
    val result = rdd2.collect() //通过调用collect来返回一个数组，然后打印输出
    result.foreach(item => println(item))
  }

  @Test
  def flatMapTest() = {
    // 1. 创建RDD
    val rdd1 = sc.parallelize(Seq("Hello 吕布", "Hello 貂蝉", "Hello 铠"))
    // 2. 处理数据
    val rdd2 = rdd1.flatMap(item => item.split(" "))
    // 3. 查看结果
    val result = rdd2.collect()
    result.foreach(item => println(item))
    // 4. 关闭资源
    sc.stop()
  }

  @Test
  def reduceByKey() = {
    // 1.创建RDD
    val rdd1 = sc.parallelize(Seq("Hello 吕布", "Hello 貂蝉", "Hello 铠"))
    // 2.处理数据
    val rdd2 = rdd1.flatMap(item => item.split(" "))
      .map(item => (item, 1))
      .reduceByKey((curr, agg) => curr + agg) //注意agg是一个临时变量，或者局部结果，起始值为0
    // 3.得到结果
    val result = rdd2.collect()
    result.foreach(item => println(item))
    // 4.关闭资源
    sc.stop()

    //上述第2步分开处理，细节查看
//    val rdd2 = rdd1.flatMap(item => item.split(" "))
//    val rdd3 = rdd2.map(item => (item, 1))
//    val rdd4 = rdd3.reduceByKey((curr, agg) => curr + agg)
//    rdd3.collect().foreach(item => println(item))
//    rdd4.collect().foreach(item => println(item))
    /*
    reduceByKey源码：
      def reduceByKey(
        func : scala.Function2[V, V, V]
      ) : org.apache.spark.rdd.RDD[scala.Tuple2[K, V]] = { /* compiled code */ }
     */
  }

}
