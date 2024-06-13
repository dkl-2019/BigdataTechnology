package SparkCore

import org.junit.Test

class Closure {

  /*
    编写一个高阶函数，在这个函数内要有一个变量，
    返回一个函数，通过这个变量完成一个计算
   */

  @Test
  def test() = {
//    val f: Int => Double = closure()
//    val area = f(5)
//    println(area)

    // 在拿到f的时候，可以通过f间接的访问到closure作用域中的内容
    // 说明f携带了一个作用域
    // 如果一个函数携带了一个外部的作用域，这种函数称之为闭包
    val f = closure()
    f(5)

    // 闭包的本质是什么？
    // f 就是闭包，闭包的本质是一个函数
    // 在 Scala 中函数是一个特殊的类型，FunctionX
    // 闭包也是一个 FunctionX 类型的对象
    // 闭包是一个对象
  }

  // 返回一个新的函数
  def closure() = {
    val factor = 3.14
    val areaFunction = (r: Int) => math.pow(r, 2) * factor
    areaFunction
  }

}













