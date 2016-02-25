import org.apache.spark.{SparkContext, SparkConf}
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author Kontopoulos Ioannis
 */
object Starter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ParallelNGG")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","64mb")
      .registerKryoClasses(Array(classOf[MergeOperator], classOf[IntersectOperator], classOf[InverseIntersectOperator], classOf[DeltaOperator], classOf[GraphSimilarityCalculator], classOf[StringEntityTokenizer], classOf[OpenNLPSentenceSplitter]))
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val start = System.currentTimeMillis

    /*val dec = new DocumentEventClustering(sc)
    val clusters = dec.getClusters("corpora")
    dec.saveClustersToCsv(clusters)*/

    val e = new StringEntity
    e.readFile(sc, "corpora/0001829", 2)
    val ss = new OpenNLPSentenceSplitter("en-sent.bin")
    ss.getSentences(e).foreach(a => println("Beg:" + a.dataStream))


    val end = System.currentTimeMillis
    println("Duration: " + (end-start).toDouble/1000 + " seconds")

  }

}
