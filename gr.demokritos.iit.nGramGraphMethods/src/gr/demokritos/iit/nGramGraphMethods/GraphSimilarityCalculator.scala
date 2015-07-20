package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
class GraphSimilarityCalculator extends SimilarityCalculator {
  
  /**
   * Gets the similarity between two graphs
   * @param g1 graph1
   * @param g2 graph2
   * @return Similarity
   */
  override def getSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]) = { 
    var gs = new GraphSimilarity(calculateSizeSimilarity(g1, g2))
    gs
  }
  
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
    if (eNum1 > eNum2) eNum2.toDouble/eNum1
    else eNum1.toDouble/eNum2
  }
  
  /**
   * Calculates value similarity
   * @param g1 graph1
   * @param g2 graph2
   * @return value similarity
   */
  def calculateValueSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Double = {
    //number of edges of graph1
    val eNum1 = g1.edges.distinct().count()
    //number of edges of graph2
    val eNum2 = g2.edges.distinct().count()
    //number of edges from maximum graph
    var max: Long = 0
    if (eNum1 > eNum2) {
      max = eNum1
      g1.edges.foreach { println }
      0.0
    }
    else {
      max = eNum2
      //TODO code here
      0.0
    }
    
  }
  
  /**
   * Calculates containment similarity
   * @param g1 graph1
   * @param g2 graph2
   * @return containment similarity
   */
  def calculateContainmentSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Double = {
    //number of edges of graph1
    val eNum1 = g1.edges.distinct().count()
    //number of edges of graph2
    val eNum2 = g2.edges.distinct().count()
    //number of edges from minimum graph
    var min: Long = 0
    if (eNum1 > eNum2) {
      min = eNum2
      val uncommonEdges: RDD[Edge[Int]] = g1.edges.subtract(g2.edges)
      val num = uncommonEdges.count()
      val c = eNum1 - num
      var sum: Double = 0
      for (1 <- 0L to c) {
        sum += 1.toDouble/min
      }
      sum
    }
    else {
      min = eNum1
      val uncommonEdges: RDD[Edge[Int]] = g2.edges.subtract(g1.edges)
      val num = uncommonEdges.distinct().count()
      val c = eNum2 - num
      var sum: Double = 0
      for (1 <- 0L to c) {
        sum += 1.toDouble/min
      }
      sum
    }
  }
  
}