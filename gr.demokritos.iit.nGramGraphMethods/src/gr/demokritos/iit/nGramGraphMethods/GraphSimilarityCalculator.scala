package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
class GraphSimilarityCalculator extends SimilarityCalculator {
  
  override def getSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]) = { /*TODO code here*/ }
  
  /**
   * Calculates size similarity
   * @param g1 graph1
   * @param g2 graph2
   * @return size similarity
   */
  def calculateSizeSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Double = {
    //number of edges of graph1
    val eNum1 = g1.edges.distinct().count()
    //number of edges of graph2
    val eNum2 = g2.edges.distinct().count()
    //calculate size similarity
    if (eNum1 > eNum2) eNum2/eNum1
    else eNum1/eNum2
  }
  
  //def calculateValueSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Double = { }
  
  //def calculateContainmentSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Double = { }
  
}