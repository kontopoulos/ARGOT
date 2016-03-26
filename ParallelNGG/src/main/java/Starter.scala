import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
object Starter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ParallelNGG")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","64mb")
      .registerKryoClasses(Array(classOf[MergeOperator], classOf[IntersectOperator], classOf[InverseIntersectOperator], classOf[DeltaOperator], classOf[GraphSimilarityCalculator], classOf[StringEntityTokenizer], classOf[OpenNLPSentenceSplitter], classOf[MatrixMCL]))
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val start = System.currentTimeMillis

    val sum = new NGGSummarizer(sc, 2)
    sum.getSummary("corpora")

    /*val ss = new OpenNLPSentenceSplitter("en-sent.bin")
    val e = new StringEntity
    e.readFile(sc, "C:\\Users\\yannis\\IdeaProjects\\ParallelNGG\\corpora\\0001179", 2)
    ss.getSentences(e).foreach(a => println(a.dataStream))*/

    /*val clusters = dec.loadClustersFromCsv("clusters.csv")
    clusters.foreach{case(k,v) =>
      println("======================================")
        v.foreach{d =>
          val e = new StringEntity
          e.readFile(sc, d, 2)
          val s = ss.getSentences(e).asInstanceOf[RDD[StringAtom]]
          s.collect.foreach(a => println(a.dataStream))
        }
    }*/

    val end = System.currentTimeMillis
    println("Duration: " + (end-start).toDouble/1000 + " seconds")

  }

}
