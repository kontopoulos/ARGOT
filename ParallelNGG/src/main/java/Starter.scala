import java.io.FileWriter

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
      .set("spark.executor.memory", "48g")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val numPartitions = args.head.toInt

    try {
      val exp = new nFoldCrossValidation(sc, numPartitions, 10)
      val start = System.currentTimeMillis
      exp.run("NaiveBayes")
      val end = System.currentTimeMillis

      val time = end - start
      val w = new FileWriter("times.log",true)
      w.write("Threads " + numPartitions + " | Time " + time)
      w.close
    }
    catch {
      case e: Exception =>
        val w = new FileWriter("errors.log",true)
        w.write(e.getMessage + "\n" + e.getStackTrace.mkString("\n"))
        w.close
    }





    /*val sum = new NGGSummarizer(sc, numPartitions, false)
    val summaries = sum.getSummary("corpora")

    sum.saveSummaries(summaries)*/


  }

}
