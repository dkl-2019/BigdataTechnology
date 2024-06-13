package SparkSQL

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.Test

class JoinProcessor {

  val spark: SparkSession = SparkSession.builder()
    .master("local[6]")
    .appName("agg processor")
    .getOrCreate()

  import spark.implicits._

  val person: DataFrame = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
    .toDF("id", "name", "cityId")
  person.createOrReplaceTempView("person")

  val cities: DataFrame = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
    .toDF("id", "name")
  cities.createOrReplaceTempView("cities")


  @Test
  def introJoin(): Unit = {

    val person = Seq((0, "Lucy", 0), (1, "Lily", 0), (2, "Tim", 2), (3, "Danial", 0))
      .toDF("id", "name", "cityId")

    val cities = Seq((0, "Beijing"), (1, "Shanghai"), (2, "Guangzhou"))
      .toDF("id", "name")

    val df = person.join(cities, person.col("cityID") === cities.col("id"))
      .select(person.col("id"),
        person.col("name"),
        cities.col("name") as "city")

    df.createOrReplaceTempView("user_city")

    spark.sql("select id, name, city from user_city where city = 'Beijing'")
      .show()
  }

  // 交叉连接
  @Test
  def crossJoin(): Unit = {

    person.crossJoin(cities)
      .where(person.col("cityId") === cities.col("id"))
      .show()

    spark.sql("select u.id, u.name, c.name from person u cross join cities c " +
      "where u.cityId = c.id")
      .show()
  }

  // 内连接
  @Test
  def inner(): Unit = {

    person.join(cities, person.col("cityId") === cities.col("id"),
      joinType = "inner")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p inner join cities c " +
      "on p.cityId = c.id")
      .show()
  }

  // 全外连接
  @Test
  def fullOutter(): Unit = {

    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "full")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p full outer join cities c " +
      "where p.cityId = c.id")
      .show()
  }

  // 左/右 外连接
  @Test
  def leftRight(): Unit = {
    // 左连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "left")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p left outer join cities c " +
      "on p.cityId = c.id")
      .show()

    // 右连接
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "right")
      .show()

    spark.sql("select p.id, p.name, c.name " +
      "from person p right outer join cities c " +
      "on p.cityId = c.id")
      .show()
  }

  @Test
  def leftAntiSemi(): Unit = {
    // 左连接 anti
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "leftanti")
      .show()

    spark.sql("select p.id, p.name " +
      "from person p left anti join cities c " +
      "on p.cityId = c.id")
      .show()

    // 左连接 semi
    person.join(cities,
      person.col("cityId") === cities.col("id"),
      joinType = "leftsemi")
      .show()

    spark.sql("select p.id, p.name " +
      "from person p left semi join cities c " +
      "on p.cityId = c.id")
      .show()
  }

}
