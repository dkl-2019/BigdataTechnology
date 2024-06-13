package SparkCore

import org.apache.commons.lang3.StringUtils
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.Test

class CacheOp {

  /*
  需求：统计文件当中 出现最多的IP次数，和最少的IP次数
    1. 创建sc
    2. 读取文件
    3. 取出 IP ，赋予初始频率
    4. 过滤清洗
    5. 统计IP出现的次数
    6. 统计出现次数最少的IP
    7. 统计出现次数最多的IP
   */
  @Test
  def prepare() = {
    // 1. 创建sc
    val conf = new SparkConf().setMaster("local[6]").setAppName("cache")
    val sc = new SparkContext(conf)
    //2. 读取文件
    val access_log = sc.textFile("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/access_log_sample.txt", minPartitions = 2)
    // 3.取出所有IP
    val countRDD = access_log.map( item => (item.split(" ")(0), 1) )
    // 4.过滤空值
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )
    // 5.统计IP出现的次数
    val aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg)
    // 6. 统计出现次数
    val lessIP = aggRDD.sortBy( item => item._2, ascending = true).first()  //升序
    val moreIP = aggRDD.sortBy( item => item._2, ascending = false).first() //降序
    println(lessIP)
    println(moreIP)

    sc.stop()
  }

  @Test
  def cache() = {
    val conf = new SparkConf().setMaster("local[6]").setAppName("cache")
    val sc = new SparkContext(conf)

    val access_log = sc.textFile("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/access_log_sample.txt", minPartitions = 2)
    val countRDD = access_log.map( item => (item.split(" ")(0), 1) )
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )
    var aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg)
    aggRDD = aggRDD.cache()

    val lessIP = aggRDD.sortBy( item => item._2, ascending = true).first()  //升序
    val moreIP = aggRDD.sortBy( item => item._2, ascending = false).first() //降序
    println((lessIP, moreIP))

    sc.stop()
  }

  @Test
  def persist() = {
    val conf = new SparkConf().setMaster("local[6]").setAppName("cache")
    val sc = new SparkContext(conf)

    val access_log = sc.textFile("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/access_log_sample.txt", minPartitions = 2)
    val countRDD = access_log.map( item => (item.split(" ")(0), 1) )
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )
    var aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg)
    aggRDD = aggRDD.persist(StorageLevel.MEMORY_ONLY)  //存储级别
    println(aggRDD.getStorageLevel)

    val lessIP = aggRDD.sortBy( item => item._2, ascending = true).first()  //升序
    val moreIP = aggRDD.sortBy( item => item._2, ascending = false).first() //降序
    println((lessIP, moreIP))

    sc.stop()
  }

  @Test
  def checkpoint() = {
    val conf = new SparkConf().setMaster("local[6]").setAppName("cache")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("checkpoint")

    val access_log = sc.textFile("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/access_log_sample.txt", minPartitions = 2)
    val countRDD = access_log.map( item => (item.split(" ")(0), 1) )
    val cleanRDD = countRDD.filter( item => StringUtils.isNotEmpty(item._1) )
    var aggRDD = cleanRDD.reduceByKey( (curr, agg) => curr + agg)

    // checkpoint
    // aggRDD = aggRDD.cache
    // 不准确说，checkpoint 是一个 Action 操作，也就是说
    // 如果调用 checkpoint，则会重新计算一下RDD，然后把觉果存在HDFS或者本地目录
    // 所以应该在checkpoint之前进行一次cache
    aggRDD = aggRDD.cache()
    aggRDD.checkpoint()

    val lessIP = aggRDD.sortBy( item => item._2, ascending = true).first()  //升序
    val moreIP = aggRDD.sortBy( item => item._2, ascending = false).first() //降序
    println((lessIP, moreIP))

    sc.stop()
  }

}
