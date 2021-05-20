import org.apache.flink.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val localLines = env.readTextFile("file:////home/siddhesh/Downloads/wordExample")
    val counts = localLines
      .flatMap {
        _.toLowerCase.split("\\W+").filter {
          _.nonEmpty
        }
      }
      .map {
        (_, 1)
      }
      .groupBy(0)
      .sum(1)
    counts.writeAsCsv("file:////home/siddhesh/Downloads/csvRecords")
    env.execute("WordCount")
    println("Done and Dusted....")
  }
}