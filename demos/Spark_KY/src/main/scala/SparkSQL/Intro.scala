package SparkSQL

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.{SparkConf, SparkContext, sql}
import org.junit.Test

class Intro {

  @Test
  def rddIntro(): Unit = {
    val config = new SparkConf().setAppName("rdd intro").setMaster("local[6]")
    val sc = new SparkContext(config)

    sc.textFile("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/wordcount.txt")
      .flatMap( _.split(" ") )
      .map( (_, 1) )
      .reduceByKey( _ + _ )
      .collect()
      .foreach(println(_))
  }


  @Test
  def dsIntro(): Unit = {
    val spark = new sql.SparkSession.Builder()
      .appName("ds intro")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val sourceRDD = spark.sparkContext.parallelize(Seq(
      Person("zhangsan", 10),
      Person("lisi", 15)
    ))
    val personDS = sourceRDD.toDS()

    val resultDS = personDS.where( 'age > 10 )
      .where( 'age < 20 )
      .select( 'name )
      .as[String]

    resultDS.show()
  }

  @Test
  def dfIntro(): Unit = {
    val spark: SparkSession = new sql.SparkSession.Builder()
      .appName("hello")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val peopleRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 9), Person("lisi", 15)))
    val peopleDS = peopleRDD.toDF()
    peopleDS.createOrReplaceTempView("people")

    val teenagers = spark.sql("select name from people where age > 10 and age < 20")

    teenagers.show()
    /*
    +----+
    |name|
    +----+
    |lisi|
    +----+
     */
  }

  @Test
  def dataset1(): Unit = {
    // 1. 创建 SparkSession
    val spark = new sql.SparkSession.Builder()
      .master("local[6]")
      .appName("dataset1")
      .getOrCreate()

    // 2. 导入隐式转换
    import spark.implicits._

    // 3. 演示
    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("list", 15)))
    val dataset = sourceRDD.toDS()

    // Dataset 支持强类型的 API
    dataset.filter( item => item.age > 10 ).show()
    // Dataset 支持弱类型 API
    dataset.filter( 'age > 10 ).show()
    dataset.filter( $"age" > 10 ).show()
    // Dataset 可以直接编写 SQL表达式
    dataset.filter( "age > 10" ).show()
  }

  @Test
  def dataset2(): Unit = {
    // 1. 创建 SparkSession
    val spark = new sql.SparkSession.Builder()
      .master("local[6]")
      .appName("dataset2")
      .getOrCreate()

    // 2. 导入隐式转换
    import spark.implicits._

    // 3. 演示
    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    val dataset = sourceRDD.toDS()

    dataset.explain(true)
  }

  @Test
  def dataset3(): Unit = {
    // 1. 创建 SparkSession
    val spark = new sql.SparkSession.Builder()
      .master("local[6]")
      .appName("dataset3")
      .getOrCreate()

    // 2. 导入隐式转换
    import spark.implicits._

    // 3. 演示
    //    val sourceRDD = spark.sparkContext.parallelize(Seq(Person("zhangsan", 10), Person("lisi", 15)))
    //    val dataset = sourceRDD.toDS()
    val dataset: Dataset[Person] = spark.createDataset(Seq(Person("zhangsan", 10), Person("lisi", 15)))

    // dataset.explain(true)
    // 无论Dataset中放置的是什么类型的对象，最终执行计划中的RDD上都是InternalRow
    // 直接获取到已经分析和解析过的Dataset的执行计划，从中拿到RDD
    val executionRdd: RDD[InternalRow] = dataset.queryExecution.toRdd

    // 通过将Dataset底层的RDD[internalRow] 通过 Decoder 转成了和 Dataset 一样的类型的 RDD
    val typedRdd: RDD[Person] = dataset.rdd

    println(executionRdd.toDebugString) //查看执行过程
    println("------------------------------------------------------------")
    println(typedRdd.toDebugString)
  }

  @Test
  def dataframe1(): Unit = {
    // 1. 创建SparkSession对象
    val spark = SparkSession.builder()
      .appName("dataframe1")
      .master("local[6]")
      .getOrCreate()

    // 2. 创建 DataFrame
    import spark.implicits._

    val dataframe: DataFrame = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDF()

    // 3. Dataframe 操作
    dataframe.where('age > 10)
      .select('name)
      .show()
  }

  @Test
  def dataframe2(): Unit = {
    val spark = SparkSession.builder()
      .appName("dataframe2")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    val personList = Seq(Person("zhangsan", 15), Person("lisi", 20))

    // 1. toDF
    val df1 = personList.toDF()
    val df2 = spark.sparkContext.parallelize(personList).toDF()

    // 2. createDataFrame
    val df3 = spark.createDataFrame(personList)

    // 3. read
    val df4 = spark.read.csv(path = "file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231_noheader.csv")
    df4.show()
  }

  @Test
  def dataframe3(): Unit = {
    // 1. 创建SparkSession
    val spark = SparkSession.builder()
      .appName("dataframe3")
      .master("local[6]")
      .getOrCreate()

    import spark.implicits._

    // 2. 读取数据
    val sourceDF = spark.read
      .option("header", value = true)
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")
    //sourceDF.show()
    // 产看 dataframe 的 scheam 信息，DataFrame中是有结构信息的
    //sourceDF.printSchema()

    // 3. 处理数据
    //    1. 列值裁剪，选择列
    //    2. 过滤掉 NA 的 PM 记录
    //    3. 分组 select year, month, count(PM_Dongsi) from ... where PM_Dongsi != NA group by year, month
    //    4. 聚合
    sourceDF.select('year, 'month, 'PM_Dongsi)
      .where('PM_Dongsi =!= "NA")
      .groupBy('year, 'month)
      .count()
      .show()

    // 是否能直接使用 SQL 语句进行查询
    //  1. 将 DataFrame 注册为临表
    sourceDF.createOrReplaceTempView("pm")
    //  2. 执行查询
    val resultDF = spark.sql("select year, month, count(PM_Dongsi) from pm where PM_Dongsi != 'NA' group by year, month")
    resultDF.show()

    spark.stop()
  }

  @Test
  def dataframe4(): Unit = {
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("dataframe4")
      .getOrCreate()

    import spark.implicits._

    val personList = Seq(Person("zhangsan", 15), Person("lisi", 20))

    // DataFrame 是弱类型的
    val df: DataFrame = personList.toDF()
    df.map( (row: Row) => Row(row.get(0), row.getAs[Int](1) * 2) )(RowEncoder.apply(df.schema))
      .show()

    // DataSet 是强类型
    val ds: Dataset[Person] = personList.toDS()
    ds.map( (person: Person) => Person(person.name, person.age * 2) )
      .show()
  }

  @Test
  def row(): Unit = {
    // 创建Person对象，也可以使用Row来表示
    val p = Person("zhangsan", 15)
    val row = Row("zhangsan", 15)

    // 获取Row中的内容
    println(row.get(0))
    println(row(1))

    // 获取时可以指定类型
    println(row.getAs[Int](1))

    // 同时 Row 也是一个样例类，可以进行 match
    row match {
      case Row(name, age) => println(name, age)
    }
  }

}

case class Person(name: String, age: Int)
