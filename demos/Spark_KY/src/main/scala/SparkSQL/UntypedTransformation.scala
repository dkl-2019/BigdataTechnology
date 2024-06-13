package SparkSQL

import org.apache.spark.sql.SparkSession
import org.junit.Test

class UntypedTransformation {
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("untyped")
    .getOrCreate()

  import spark.implicits._

  @Test
  def select() = {
    val ds = Seq(Person("zhangsan", 12), Person("lisi", 18), Person("zhangsan", 8)).toDS()

    ds.select('name).show()
    ds.selectExpr("sum(age)").show()  //聚合

    import org.apache.spark.sql.functions._
    ds.select(expr("sum(age)")).show()
  }

  @Test
  def column() = {
    val ds = Seq(Person("zhangsan", 12), Person("lisi", 18), Person("zhangsan", 8)).toDS()

    import org.apache.spark.sql.functions._
    // 如果想使用函数功能
    //  1. 使用 functions.xx
    //  2. 使用表达式，可以使用 expr("...")
    ds.withColumn("random", expr("rand()")).show()
    ds.withColumn("name_new", 'name).show()  // 新增，重命名列名
    ds.withColumn("name_jok", 'name === "").show()
    ds.withColumnRenamed("name", "new_name").show() // 重命名
  }

  @Test
  def groupBy() = {
    val ds = Seq(Person("zhangsan", 12), Person("zhangsan", 8), Person("lisi", 15)).toDS()

    // 为什么 groupByKey 是有类型的 ？？？
    ds.groupByKey( item => item.name )
    // 为什么 groupBy 是无类型的，因为 groupBy 所生成的对象中的算子是无类型的，针对列进行处理
    import org.apache.spark.sql.functions._
    ds.groupBy('name).agg(mean("age")).show()
  }

}







