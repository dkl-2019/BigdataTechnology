package SparkSQL

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StructField, StructType}
import org.junit.Test

class NullProcessor {

  // 1. 创建 SparkSession
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("null processor")
    .getOrCreate()

  import spark.implicits._

  @Test
  def nullAndNaN() = {

    // 2. 读取数据集
    /*
        1. 通过自动推断类型来读取
          spark.read
            .option("header", true)
            .option("inferSchema", true)
            .csv()
        2. 直接读取字符串，在后续的操作中使用 map 算子转类型
          spark.read.csv().map( row => row... )
        3. 指定 Schema， 不要自动推断
     */
    val schema = StructType(
      List(
        StructField("id", LongType),
        StructField("year", IntegerType),
        StructField("month", IntegerType),
        StructField("day", IntegerType),
        StructField("hour", IntegerType),
        StructField("season", IntegerType),
        StructField("pm", DoubleType)
      )
    )

    val sourceDF = spark.read
      .option("header", value = true)
      .schema(schema)
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/beijingpm_with_nan.csv")

    sourceDF.show()

    /*
     4. 丢弃
        规则：
            1. any：只要有 NaN 就丢弃
            2. all：所有都是 NaN 的行才丢弃
            3. 某些列的规则
    */
    sourceDF.na.drop("any").show()  //不指定默认也是any
    sourceDF.na.drop("all").show()
    sourceDF.na.drop("any", List("year", "month", "day", "hour")).show()

    /*
      5. 填充
          规则：
              1. 针对所有列数据进行默认值填充
              2. 针对特定列填充
    */
    sourceDF.na.fill(0).show()
    sourceDF.na.fill(0, List("year", "month")).show()

  }

  @Test
  def strProcessor() = {
    // 读取数据集
    val sourceDF = spark.read
      .option("header", value = true)
      .option("inferSchema", value = true)
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/BeijingPM20100101_20151231.csv")

    // sourceDF.show()

    // 1. 丢弃
    sourceDF.where('PM_Dongsi =!= "NA").show()

    // 2. 替换
    import org.apache.spark.sql.functions._

    sourceDF.select(
      'No as "id", 'year, 'month, 'day, 'hour, 'season,
      when('pM_Dongsi === "NA", Double.NaN)
        .otherwise('PM_Dongsi cast DoubleType)
        .as("pm")
    ).show()

    // 原类型和转换过后的类型，必须一致
    sourceDF.na.replace("PM_Dongsi", Map("NA" -> "NaN")).show()

  }

}
