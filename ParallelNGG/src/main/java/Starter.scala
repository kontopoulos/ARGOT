import java.util.Calendar

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
      .registerKryoClasses(Array(classOf[MergeOperator], classOf[IntersectOperator], classOf[InverseIntersectOperator], classOf[DeltaOperator], classOf[GraphSimilarityCalculator], classOf[StringEntityTokenizer]))
      .set("spark.executor.memory", "2g")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    /*val e = new StringEntity
    e.readFile(sc, "corpora/0000962", 2)
    val nggc = new WordNGramGraphCreator(sc, 2, 3, 3)
    val g = nggc.getGraph(e)
    println(g.numEdges + " " + g.numVertices)
    g.edges.foreach(println)
    g.vertices.foreach(println)*/


    /*val e1 = new StringEntity
    e1.readFile(sc, "corpora/0000493", 2)
    val e2 = new StringEntity
    e2.readFile(sc, "corpora/0000962", 2)
    val nggc = new NGramGraphCreator(sc, 2, 3, 3)
    val gsc = new GraphSimilarityCalculator
    val g1 = nggc.getGraph(e1)
    val g2 = nggc.getGraph(e2)
    val gs = gsc.getSimilarity(g1, g2)
    println(gs.getSimilarityComponents("containment") + " " + gs.getSimilarityComponents("value") + " " + gs.getSimilarityComponents("normalized") + " " + gs.getSimilarityComponents("size"))
*/

    /*val e = new StringEntity
    e.readStringRDDFromFile(sc, "alum/0000191", 2)
    val tokenizer = new StringEntityTokenizer
    tokenizer.getWordNGrams(e, 2).foreach(atom => println(atom.dataStream))*/

    /*val calendar = Calendar.getInstance
    val now = calendar.getTime
    println(new java.sql.Timestamp(now.getTime))


    val dec = new DocumentEventClustering(sc)
    val clusters = dec.getClusters("corpora")
    dec.saveClustersToCsv(clusters)


    val calendar2 = Calendar.getInstance
    val now2 = calendar2.getTime
    println(new java.sql.Timestamp(now2.getTime))*/

  }

}
