package cn.itcast.spark.search

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object mydemo {

  def main(args: Array[String]): Unit = {
    // 构建Spark Application应用层入口实例对象
    val sc: SparkContext = {
      // a. 创建SparkConf对象，设置应用信息
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      // b. 传递SparkConf对象，创建实例
      SparkContext.getOrCreate(sparkConf)
    }

    // TODO：1.加载数据
    val sogouRDD: RDD[String] = sc.textFile("/datas/sogou/SogouQ.reduced", minPartitions = 2) //设置最小分区数为2
    println(sogouRDD.count()) //产看文件中有多少条日志
    println(sogouRDD.first()) //查看文件中第一条日志

    // TODO：2.解析数据，封装到CaseClass样例类中
    val etlRDD: RDD[SogouRecord] = sogouRDD
      .filter(line => null != line && line.trim.split("\\s+").length == 6)
      .mapPartitions{iter =>
        iter.map{line =>
          val array = line.trim.split("\\s+")
          // 构建SogouRecord对象
          SogouRecord(
            array(0), array(1), //
            array(2).replaceAll("\\[|\\]", ""), //
            array(3).toInt, array(4).toInt, array(5)
          )
        }
      }
    println(etlRDD.first())

  }

}
