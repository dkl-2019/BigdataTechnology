package SparkSQL

import org.apache.spark.sql.{DataFrameReader, SaveMode, SparkSession}
import org.junit.Test

class ReadWrite {

  //System.setProperty("hadoop.home.dir", "C:\\winutils")
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("write1")
    .getOrCreate()

  @Test
  def reader1() = {
    // 1. 创建一个 SparkSessioon
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader1")
      .getOrCreate()

    // 2. 框架在哪
    val reader: DataFrameReader = spark.read

  }

  @Test
  def reader2() = {
    // 1. 创建一个 SparkSessioon
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("reader2")
      .getOrCreate()

    // 2.1  第一种读取形式
    spark.read
      .format("csv")  // 指定文件类型
      .option("header", value = true) // 显示列名
      .option("inferSchema", value = true)
      .load("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")
      .show(10)

    // 2.2  第二种读取形式
    spark.read
      .option("header", value = true) // 显示列名
      .option("inferSchema", value = true)
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")
      .show(10)
  }

  @Test
  def write1() = {
    //System.setProperty("hadoop.home.dir", "C:\\winutils")

    // 1. 创建一个 SparkSessioon
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("write1")
      .getOrCreate()

    // 2. 读取一个数据集
    val df = spark.read.option("header", true).csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")

    // 3. 写入数据集
    df.write.json("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijing_pm.json")

    df.write.format("json").save("fileZ:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijing_pm2.json")

  }

  @Test
  def parquet() = {
    // 1. 读取 csv 文件数据
    val df = spark.read.option("header", true).csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")

    // 2. 将数据写为 parquet格式
    // 写入的时候默认格式就是parquet
    // 写入模式：报错、覆盖、追加、忽略
    df.write
      .format("parquet")  
      .mode(SaveMode.Overwrite) //覆盖重写
      .save("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijing_pm3")

    // 3. 读取 parquet 格式
    // 默认读取格式是parquet，并且可以读取文件夹
    spark.read
      .load("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijing_pm3")
      .show()
  }

  @Test
  def parquetPartitions() = {
    // 1. 读取数据
    val df = spark.read
      .option("header", true)
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")

    // 2. 写文件，表分区
    df.write
      .partitionBy("year", "month")
      .save("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijing_pm4")
    
    // 3. 读文件，自动发现分区
    // 写分区表的时候，分区列不会包含在生成的文件中
    // 直接通过文件来进行读取的话，分区信息会丢失
    // spark sql 会自动发现分区读取
    spark.read
      .parquet("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijing_pm4")
      .printSchema()
  }


  @Test
  def json() = {
    val df = spark.read
      .option("header", true)
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")

//    df.write
//      .json("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijing_pm5.json")

    spark.read
      .json("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijing_pm5.json")
      .show()
  }

  /*
    toJSON应用场景：
      处理完以后，DataFrame中如果是一个对象，如果其它系统和只支持 JSON 格式数据，
      SparkSQL 如果和这种系统进行整合的时候，就需要进行转换
   */
  @Test
  def json1() = {
    val df = spark.read
      .option("header", true)
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")

    df.toJSON.show()
  }

  /*
    从消息队列中取出 JSON 格式的数据，需要使用 SparkSQL 进行处理
   */
  @Test
  def json2() = {
    val df = spark.read
      .option("header", true)
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")

    val jsonRDD = df.toJSON.rdd

    spark.read.json(jsonRDD).show()
  }

}
