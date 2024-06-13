package SparkSQL

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

/*
  MySQL 的访问方式：
    1. 使用本地运行
    2. 提交到集群运行

  写入 MySQL 数据时，使用本地运行，读取的时候使用集群运行
 */

object MySQLWrite {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession 对象
    val spark = SparkSession.builder()
      .master("local[6]")
      .appName("MySQLWrite")
      .getOrCreate()

    // 2. 读取数据，创建 DataFrame
    //    1. 拷贝文件
    //    2. 读取
    val schema = StructType(
      List(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )

    val df = spark.read
      .schema(schema)
      .option("delimiter", "\t")
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/studenttab10k")

    // 3. 处理数据
    val resultDF = df.where("age < 30")
    resultDF.show()

    // 4. 落地数据
    resultDF.write
      .format("jdbc")
      .option("url", "jdbc:mysql://192.168.1.183:3306/spark02")
      .option("dbtable", "student")
      .option("user", "spark03")
      .option("password", "Spark03!")
      .mode(SaveMode.Overwrite)
      .save()
  }

}



