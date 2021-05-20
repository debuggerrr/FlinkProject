import org.apache.flink.api.scala._

object FilterOnWords {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val localLines = env.readTextFile("file:////home/siddhesh/Downloads/wc.txt")
    val filteredWords = localLines.distinct().filter(_.startsWith("N"))
    filteredWords.print()
  }
}
