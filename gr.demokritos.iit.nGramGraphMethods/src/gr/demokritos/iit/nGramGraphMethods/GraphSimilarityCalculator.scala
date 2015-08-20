package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.graphx.Graph

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
  override def getSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Similarity = {
    val gs = new GraphSimilarity(calculateSizeSimilarity(g1, g2), calculateValueSimilarity(g1, g2), calculateContainmentSimilarity(g1, g2), calculateValueSimilarity(g1, g2)/calculateSizeSimilarity(g1, g2))
    gs
  }

  /**
   * Calculates size similarity
   * @param g1 graph1
   * @param g2 graph2
   * @return size similarity
   */
  private def calculateSizeSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Double = {
    //number of edges of graph1
    val eNum1 = g1.edges.distinct.count
    //number of edges of graph2
    val eNum2 = g2.edges.distinct.count
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
  private def calculateValueSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Double = {
    //number of edges of graph1
    val eNum1 = g1.edges.distinct.count
    //number of edges of graph2
    val eNum2 = g2.edges.distinct.count
    //holds the number of edges from larger graph
    var max: Long = 0
    //get the number of edges of larger graph
    if (eNum1 > eNum2) max = eNum1
    else max = eNum2
    //min1 holds the smallest number of edges from graph1
    var min1: Double = Double.MaxValue
    //min2 holds the smallest number of edges from graph2
    var min2: Double = Double.MaxValue
    //max1 holds the bigger number of edges from graph1
    var max1: Double = 0.0
    //max2 holds the bigger number of edges from graph2
    var max2: Double = 0.0
    //find min edge weight of graph1
    g1.edges.collect.foreach{ e => if (e.attr < min1) min1 = e.attr }
    //find min edge weight of graph2
    g2.edges.collect.foreach{ e => if (e.attr < min2) min2 = e.attr }
    //find max edge weight of graph1
    g1.edges.collect.foreach{ e => if (e.attr > max1) max1 = e.attr }
    //find max edge weight of graph2
    g2.edges.collect.foreach{ e => if (e.attr > max2) max2 = e.attr }
    //maximum holds the largest weight between the two max weights
    var maximum: Double = 0.0
    //minimum holds the smallest weight between the two min weights
    var minimum: Double = 0.0
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
  private def calculateContainmentSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Double = {
    //number of edges of graph1
    val eNum1 = g1.edges.distinct.count
    //number of edges of graph2
    val eNum2 = g2.edges.distinct.count
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