package cn.itcast.spark.func.agg

object Reduce_Test {

  def main(args: Array[String]): Unit = {
    // 构建List实例对象
    val list:List[Int] = (1 to 10).toList
    println(list)

    // reduce 函数
    val result = list.reduce((tmp, item) => {
      println(s"tep = $tmp, item = $item, sum = ${tmp + item}")
      tmp + item
    })
  }

}

