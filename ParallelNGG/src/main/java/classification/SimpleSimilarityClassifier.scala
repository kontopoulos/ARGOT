import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
class SimpleSimilarityClassifier(val sc: SparkContext, val numPartitions: Int) extends CustomClassifier {

  /**
   * Train the system based on a dataset
   * @param trainset array of files
   * @return class graph
   */
  override def train(trainset: Array[String]): Graph[String, Double] = {
    val nggc = new NGramGraphCreator(sc, numPartitions, 3, 3)
    val m = new MergeOperator(0.5)
    val e1 = new StringEntity
    e1.readFile(sc, trainset(0), numPartitions)
    val e2 = new StringEntity
    e2.readFile(sc, trainset(1), numPartitions)
    val g1 = nggc.getGraph(e1)
    val g2 = nggc.getGraph(e2)
    var merged = m.getResult(g1, g2)
    for (i <- 2 to trainset.length-1) {
      val e = new StringEntity
      e.readFile(sc, trainset(i), numPartitions)
      val g = nggc.getGraph(e)
      if (i % 30 == 0) {
        //materialize and store for future use
        merged.edges.distinct.cache
        //merged.edges.distinct.checkpoint
        merged.edges.distinct.count
        //every 30 iterations cut the lineage, due to long iteration
        merged = Graph(merged.vertices.distinct, merged.edges.distinct)
      }
      merged = m.getResult(merged, g)
    }
    merged
  }

  /**
   * Tests current document with class graphs
   * @param f document to be tested
   * @param graphs list of class graphs
   * @return list of labels
   */
  override def test(f: String, graphs: List[Graph[String, Double]]): List[String] = {
    val nggc = new NGramGraphCreator(sc, numPartitions, 3, 3)
    val e = new StringEntity
    e.readFile(sc, f, numPartitions)
    val testGraph = nggc.getGraph(e)
    val gsc = new GraphSimilarityCalculator
    //taking into account the sum of value, normalized value and containment similarities in every case
    //test with first class
    val gs1 = gsc.getSimilarity(testGraph, graphs(0))
    val simil01 = gs1.getSimilarityComponents("value") + gs1.getSimilarityComponents("normalized") + gs1.getSimilarityComponents("containment")
    //test with second class
    val gs2 = gsc.getSimilarity(testGraph, graphs(1))
    val simil02 = gs2.getSimilarityComponents("value") + gs2.getSimilarityComponents("normalized") + gs2.getSimilarityComponents("containment")
    //evaluate and return predicted label
    var labels: List[String] = Nil
    if (simil01 > simil02) {
      labels :::= List("C01")
    }
    else {
      labels :::= List("C02")
    }
    labels
  }

}
