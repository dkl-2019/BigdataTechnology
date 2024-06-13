package SparkSQL

import java.lang

import org.apache.spark.sql.{DataFrame, Dataset, KeyValueGroupedDataset, SparkSession}
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
import org.junit.Test

class TypedTransformation {
  // 创建 SparkSession
  val spark = SparkSession.builder()
    .master("local[6]")
    .appName("trans")
    .getOrCreate()

  import spark.implicits._

  @Test
  def trans() = {
    // flatMap
    val ds1 = Seq("hello spark", "hello hadoop").toDS()
    ds1.flatMap( item => item.split(" ") )
      .show()

    // map
    val ds2 = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDS()
    ds2.map(person => Person(person.name, person.age * 2))
      .show()

    // mapPartition
    ds2.mapPartitions(
      // iter 不能大到每个 Executor 的内存放不下，不然就会 OOM
      // 对每个元素进行转换，后生成一个新的集合
      iter => {
        val result = iter.map(person => Person(person.name, person.age * 2))
        result
      }
    ).show()

  }

  @Test
  def trans1() = {
    val ds = spark.range(10)
    ds.show()
    ds.transform( dataset => dataset.withColumn("doubled", 'id * 2) )
      .show()
  }

  @Test
  def as() = {
    val structType = StructType(
      Seq(
        StructField("name", StringType),
        StructField("age", IntegerType),
        StructField("gpa", FloatType)
      )
    )

    val soruceDF: DataFrame = spark.read
      .schema(structType)
      .option("delimiter", "\t")
      .csv("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/studenttab10k")

    val dataset: Dataset[Student] = soruceDF.as[Student]
    dataset.show()
  }

  @Test
  def filter() = {
    val ds = Seq(Person("zhangsan", 15), Person("lisi", 20)).toDS()
    ds.filter( person => person.age > 15 )
      .show()
  }

  @Test
  def groupByKey() = {
    val ds = Seq(Person("zhangsan", 15), Person("zhangsan", 16), Person("lisi", 20)).toDS()

    val grouped: KeyValueGroupedDataset[String, Person] = ds.groupByKey(person => person.name )
    val result: Dataset[(String, Long)] = grouped.count()
    result.show()
  }

  @Test
  def split() = {
    val ds = spark.range(15)
    // randomSplit，切多少份，权重多少
    val datasets: Array[Dataset[lang.Long]] = ds.randomSplit(Array(5, 2, 3))
    datasets.foreach(_.show())
    // sample
    ds.sample(false, fraction = 0.4).show()
  }

  @Test
  def sort() = {
    val ds = Seq(Person("zhangsan", 15), Person("zhangsan", 16), Person("lisi", 20)).toDS()
    ds.orderBy('age.desc).show()
    ds.sort('age.asc).show()
  }

  @Test
  def dropDuplicates() = {
    val ds = Seq(Person("zhangsan", 15), Person("zhangsan", 15), Person("lisi", 20)).toDS()
    ds.distinct().show()
    ds.dropDuplicates("age").show()
  }

  @Test
  def collection() = {
    val ds1 = spark.range(1, 10)
    val ds2 = spark.range(5, 15)

    // 差集
    ds1.except(ds2).show()
    // 交集
    ds1.intersect(ds2).show()
    // 并集
    ds1.union(ds2).show()
    // limit
    ds1.limit(3).show()
  }

}

case class Student(name: String, age: Int, gpa:Float)
