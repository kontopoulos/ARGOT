import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
class NGramGraphSimilarityClassifier(val sc: SparkContext) extends Classifier {

  /**
   * Train the system based on a dataset
   * @param ens list of entities
   * @return class graph
   */
  override def train(ens: List[Entity]): Graph[String, Double] = {
    println("Training...")
    var graphs: List[Graph[String, Double]] = Nil
    val es = ens.asInstanceOf[List[StringEntity]]
    val nggc = new NGramGraphCreator(sc)
    //create graphs from entities
    es.foreach{ e =>
      val g = nggc.getGraph(e, 3, 3)
      graphs :::= List(g)
    }
    val m = new GraphMerger(0.5)
    //merge graphs to a class graph
    var merged = m.getResult(graphs(0), graphs(1))
    for (i <- 2 to graphs.size-1) {
      if (i % 30 == 0) {
        //every 30 iterations cut the lineage, due to long iteration
        merged = Graph(merged.vertices.distinct, merged.edges.distinct)
      }
      merged = m.getResult(merged, graphs(i))
    }
    println("Training complete.")
    merged
  }

  /**
   * Test the current entity to a class graph
   * @param e entity to be tested
   * @param graphs list of class graphs
   * @return list of labels
   */
  override def test(e: Entity, graphs: List[Graph[String, Double]]): List[String] = {
    val en = e.asInstanceOf[StringEntity]
    val nggc = new NGramGraphCreator(sc)
    val testGraph = nggc.getGraph(en, 3, 3)
    //first class graph
    val g01 = graphs(0)
    //second class graph
    val g02 = graphs(1)
    //third class graph
    val g03 = graphs(2)
    val gsc = new GraphSimilarityCalculator
    //taking into account the sum of value and normalized value similarities in every case
    //test with first class
    val gs1 = gsc.getSimilarity(testGraph, g01)
    val simil01 = gs1.getSimilarityComponents("value") + gs1.getSimilarityComponents("normalized")
    //test with second class
    val gs2 = gsc.getSimilarity(testGraph, g02)
    val simil02 = gs2.getSimilarityComponents("value") + gs2.getSimilarityComponents("normalized")
    //test with third class
    val gs3 = gsc.getSimilarity(testGraph, g03)
    val simil03 = gs3.getSimilarityComponents("value") + gs3.getSimilarityComponents("normalized")
    //evaluate and return predicted label
    if (simil01 > simil02) {
      if (simil01 > simil03) {
        var labels: List[String] = Nil
        labels :::= List("C01")
        labels
      }
      else {
        var labels: List[String] = Nil
        labels :::= List("C03")
        labels
      }
    }
    else {
      if (simil02 > simil03) {
        var labels: List[String] = Nil
        labels :::= List("C02")
        labels
      }
      else {
        var labels: List[String] = Nil
        labels :::= List("C03")
        labels
      }
    }
  }
}
