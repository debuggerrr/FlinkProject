import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)
    val counts = text.flatMap {
      _.toLowerCase.split("\\W+") filter {
        _.nonEmpty
      }
    }
      .map {
        (_, 1)
      }
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1) //sum(1) means the values at position 1 in the tuple will be aggregated on the basis of each key.

    counts.print()

    env.execute("Window Stream WordCount")
  }
}
//use nc -l 9999 on terminal to start socket and then you can enter the dataset as needed.