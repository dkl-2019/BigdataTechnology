package cn.kaizi.flink.scalademos

import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object Wordcount {

  def main(args: Array[String]): Unit = {

//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//
//    val text = env.socketTextStream("LAPTOP-EN35HFH3", 9999)
//
//    val counts = text.flatMap { _.toLowerCase.split("\\s+") filter { _.nonEmpty } }
//      .map { (_, 1) }
//      .keyBy(_._1)
//      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//      .sum(1)
//    counts.print()
//    env.execute("Window Stream WordCount")

  }

}
