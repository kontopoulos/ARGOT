import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author Kontopoulos Ioannis
 */
object Starter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ParallelNGG")
      //.setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","64mb")
      .registerKryoClasses(Array(classOf[MergeOperator], classOf[IntersectOperator], classOf[InverseIntersectOperator], classOf[DeltaOperator], classOf[GraphSimilarityCalculator], classOf[StringEntityTokenizer], classOf[OpenNLPSentenceSplitter], classOf[MatrixMCL]))
      .set("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val numPartitions = args.head.toInt

    val sum = new NGGSummarizer(sc, numPartitions, false)
    val summaries = sum.getSummary("corpora")

    sum.saveSummaries(summaries)


  }

}
