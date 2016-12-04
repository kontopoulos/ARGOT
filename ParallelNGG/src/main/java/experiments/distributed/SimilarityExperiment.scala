package experiments.distributed

import graph.NGramGraphCreator
import graph.operators.MultiGraphMergeOperator
import graph.similarity.GraphSimilarityCalculator
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import structs.StringEntity
import traits.CustomClassifier

/**
 * @author Kontopoulos Ioannis
 */
class SimilarityExperiment(val sc: SparkContext, val numPartitions: Int) extends CustomClassifier {

  /**
   * Train the system based on a dataset
   * @param trainset array of graphs
   * @return class graph
   */
  override def train(trainset: Array[Graph[String, Double]]): Graph[String, Double] = {
    val mo = new MultiGraphMergeOperator(sc,numPartitions)
    mo.getResult(trainset)
  }

  /**
   * Tests current document with class graphs
   * @param f document to be tested
   * @param graphs list of class graphs
   * @return list of labels
   */
  override def test(f: String, graphs: Array[Graph[String, Double]]): Array[String] = {
    val nggc = new NGramGraphCreator(sc,3, 3)
    val e = new StringEntity
    e.fromFile(f)
    val testGraph = nggc.getGraph(e,numPartitions)
    val gsc = new GraphSimilarityCalculator
    //taking into account the sum of value, normalized value and containment similarities in every case
    //test with first class
    val gs1 = gsc.getSimilarity(testGraph, graphs.head)
    val simil01 = gs1.getSimilarityComponents("value") + gs1.getSimilarityComponents("normalized") + gs1.getSimilarityComponents("containment")
    //test with second class
    val gs2 = gsc.getSimilarity(testGraph, graphs(1))
    val simil02 = gs2.getSimilarityComponents("value") + gs2.getSimilarityComponents("normalized") + gs2.getSimilarityComponents("containment")
    //evaluate and return predicted label
    var labels = Array.empty[String]
    if (simil01 > simil02) {
      labels :+= "C01"
    }
    else {
      labels :+= "C02"
    }
    labels
  }

}
