import org.apache.flink.api.scala._

object JoinsExample {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds1: DataSet[(Int, String)] = env.readTextFile("file:////home/siddhesh/Downloads/wordExample").map(_.split(",")).
      map { x => (x(0).toInt, x(1)) }
    val ds2: DataSet[(Int, String)] = env.readTextFile("file:////home/siddhesh/Downloads/wc.txt").map(_.split(",")).
      map { x => (x(0).toInt, x(1)) }
    val ds3: DataSet[(Int, String, String)] = ds1.join(ds2).where(0).equalTo(0) { (l, r) => (l._1, l._2, r._2) }
    ds3.map { tuple => tuple._1 + " , " + tuple._2 + " , " + tuple._3 }
      .writeAsText("file:///home/siddhesh/Downloads/csvdata1").setParallelism(1) //writes the output to a single file instead of multiple part files.
    print("Done and Dusted...")
    env.execute()
  }
}
