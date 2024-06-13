package SparkCore

import org.apache.spark.{SparkConf, SparkContext}

object SogouLogTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sogoulogs").setMaster("local[6]")
    val sc = new SparkContext(conf)

    val sougou_data = sc.textFile("file:///F:/Spark_Maven_Project/Spark/bigdata-spark/datasets/SogouQ.reduced", minPartitions = 2)
    println(sougou_data.first())

    sougou_data.filter(line => line.trim.split("\\s+").length == 6 && line != null)


  }

}
