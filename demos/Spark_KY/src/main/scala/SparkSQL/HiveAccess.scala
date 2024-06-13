package SparkSQL

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object HiveAccess {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession
    //    1. 开启 Hive 支持
    //    2. 指定 Metastore 的位置
    //    3. 指定 Warehourse 的位置
    val spark = SparkSession.builder()
      .appName("hive access1")
      .enableHiveSupport()  // 开启 Hive 支持
      .config("hive.metastore.uris", "thirft://192.168.1.183:9083")  // Metastore
      .config("spark.sql.warehouse.dir", "hdfs://192.168.1.183:9000/dataset/hive") // Warehourse
      .getOrCreate()

    import spark.implicits._

    // 2. 读取数据
    //    1. 读取HDFS，因为在集群中执行，没办法保证在哪台机器中执行，
    //        所以，要把文件上传到所有机器中，才能读取本地文件
    //        上传到HDFS中就可以解决这个问题，所有的机器都可以读取HDFS中的文件
    //    2. 使用 DF 读取数据

    val schema = StructType(
      List(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )

    val dataframe = spark.read
      .option("delimiter", "\t")
      .schema(schema)
      .csv("hdfs://192.168.1.183:9000/dataset/studenttab10k")

    val resultDF = dataframe.where('age > 50)

    // 3. 写入数据，写入表的 API ，saveAsTable
    resultDF.write.mode(SaveMode.Overwrite).saveAsTable("spark03.student")

  }

}
