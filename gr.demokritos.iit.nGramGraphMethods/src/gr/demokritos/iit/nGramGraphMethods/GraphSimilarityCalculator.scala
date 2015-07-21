package gr.demokritos.iit.nGramGraphMethods

import org.apache.commons.math.stat.descriptive.rank.Max
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
    val eNum1 = g1.numEdges
    //number of edges of graph2
    val eNum2 = g2.numEdges
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
    val eNum1 = g1.numEdges
    //number of edges of graph2
    val eNum2 = g2.numEdges
    //holds the number of edges from larger graph
    var max: Long = 0
    //get the number of edges of larger graph
    if (eNum1 > eNum2) max = eNum1
    else max = eNum2
    var min1: Int = Int.MaxValue
    var min2: Int = Int.MaxValue
    var max1: Int = 0
    var max2: Int = 0
    //find min edge weight of graph1
    for (i <- 1L to g1.numEdges) {
      if (!g1.edges.filter { case Edge(src, dst, attr) => attr < min1 }.isEmpty()) {
        min1 = g1.edges.filter { case Edge(src, dst, attr) => attr < min1 }.first().attr
      }
    }
    //find min edge weight of graph2
    for (i <- 1L to g2.numEdges) {
      if (!g2.edges.filter { case Edge(src, dst, attr) => attr < min2 }.isEmpty()) {
        min2 = g2.edges.filter { case Edge(src, dst, attr) => attr < min2 }.first().attr
      }
    }
    //find max edge weight of graph1
    for (i <- 1L to g1.numEdges) {
      if (!g1.edges.filter { case Edge(src, dst, attr) => attr > max1 }.isEmpty()) {
        max1 = g1.edges.filter { case Edge(src, dst, attr) => attr > max1 }.first().attr
      }
    }
    //find max edge weight of graph2
    for (i <- 1L to g2.numEdges) {
      if (!g2.edges.filter { case Edge(src, dst, attr) => attr > max2 }.isEmpty()) {
        max2 = g2.edges.filter { case Edge(src, dst, attr) => attr > max2 }.first().attr
      }
    }
    var maximum: Long = 0
    var minimum: Long = 0
    if(min1 < min2) minimum = min1
    else minimum = min2
    if(max1 > max2) maximum = max1
    else maximum = max2
    val srcDst1 = g1.edges.map(e => (e.srcId, e.dstId))
    val srcDst2 = g2.edges.map(e => (e.srcId, e.dstId))
    //c holds the number of common edges
    val c = srcDst1.intersection(srcDst2).count()
    //for each common edge add to a sum
    var sum: Double = 0.0
    for (i <- 1L to c) {
      sum += (minimum.toDouble/maximum)/max
    }
    sum
  }

  /**
   * Calculates containment similarity
   * @param g1 graph1
   * @param g2 graph2
   * @return containment similarity
   */
  def calculateContainmentSimilarity(g1: Graph[(String, Int), Int], g2: Graph[(String, Int), Int]): Double = {
    //number of edges of graph1
    val eNum1 = g1.numEdges
    //number of edges of graph2
    val eNum2 = g2.numEdges
    //number of edges from minimum graph
    var min: Long = 0
    val srcDst1 = g1.edges.map(e => (e.srcId, e.dstId))
    val srcDst2 = g2.edges.map(e => (e.srcId, e.dstId))
    //c holds the number of common edges
    val c = srcDst1.intersection(srcDst2).count()
    //get the number of edges of smallest graph
    if (eNum1 > eNum2) min = eNum2
    else min = eNum1
    //for each common edge add 1/min to a sum
    var sum: Double = 0
    for (i <- 1L to c) {
      sum += 1.toDouble/min
    }
    sum
  }

}