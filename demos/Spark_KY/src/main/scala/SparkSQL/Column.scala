package SparkSQL

import org.apache.spark.sql
import org.apache.spark.sql.{ColumnName, Dataset, SparkSession}
import org.junit.Test

class Column {

  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("column")
    .getOrCreate()

  import spark.implicits._

  @Test
  def creation() = {
    val ds = Seq(Person("zhangsan", 15), Person("lisi", 10)).toDS()
    val df = Seq(("zhangsan", 15), ("lisi", 10)).toDF("name", "age")

    // ' 必须导入spark的隐式转换才可以使用 str.intern()
    val column: Symbol = 'name

    // $ 必须导入spark的隐式转换才可以使用
    val column1: ColumnName = $"name"

    // col 必须导入 functions
    import org.apache.spark.sql.functions._
    val column2: sql.Column = col("name")

    // column 必须导入 functions
    val column3: sql.Column = column("name")

    // 以上四种创建方式，没有关联的 Dataset
    ds.select(column).show()
    df.select(column).show()

    // Dataset 可以，DataFrame 可以使用 Column 对象选中
    // select 方法可以使用 column 对象来选中某个列，其它的算子同样可以
    df.where(column === "zhangsan").show()

    // dataset.col
    // 使用 dataset 来获取 column 对象，会和某个 Dataset 进行绑定，在逻辑计划中，就会有不同的表现
    val column4 = ds.col("name")
    val column5 = ds.col("name")

    // dataset.apply
    val column6: sql.Column = ds.apply("name")

  }

  @Test
  def as() = {
    val ds: Dataset[Person] = Seq(Person("zhangsan", 15), Person("lisi", 10)).toDS()

    // 起别名
    ds.select('name as "new_name").show()
    // 类型转换
    ds.select('age.as[Long]).show()
  }

  @Test
  def api() = {
    val ds: Dataset[Person] = Seq(Person("zhangsan", 15), Person("lisi", 10)).toDS()

    // 需求1： ds 增加列，双倍年龄
    // 'age * 2 本质上就是将一个表达式（逻辑计划表达式）附着到 column 对象上
    // 表达式在执行的时候对应每一条数据进行操作
    ds.withColumn("doubled", 'age * 2).show()

    // 需求2： 模糊查询。通配符
    ds.where('name like "zhang%").show()

    // 需求3： 排序，正反序
    ds.sort('age asc).show()

    // 需求4： 枚举判断
    ds.where('name isin("zhangsan", "lisi", "xiaopang")).show()
  }


}




