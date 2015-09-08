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
    val gs = new GraphSimilarity(calculateSizeSimilarity(g1, g2), calculateValueSimilarity(g1, g2), calculateContainmentSimilarity(g1, g2))
    gs
  }

  /**
   * Calculates size similarity
   * @return size similarity
   */
  private def calculateSizeSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Double = {
    //number of edges of graph1
    val g1EdgeCount = g1.edges.distinct.count
    //number of edges of graph2
    val g2EdgeCount = g2.edges.distinct.count
    val sSimil = Math.min(g1EdgeCount, g2EdgeCount).toDouble/Math.max(g1EdgeCount, g2EdgeCount)
    sSimil
  }

  /**
   * Calculates value similarity
   * @param g1 graph1
   * @param g2 graph2
   * @return value similarity
   */
  private def calculateValueSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Double = {
    //number of edges of graph1
    val g1EdgeCount = g1.edges.distinct.count
    //number of edges of graph2
    val g2EdgeCount = g2.edges.distinct.count
    //common edges are the ones with common vertices pairs
    val srcDst1 = g1.edges.distinct.map(e => (e.srcId, e.dstId))
    val srcDst2 = g2.edges.distinct.map(e => (e.srcId, e.dstId))
    //c holds the number of common edges
    val c = srcDst1.intersection(srcDst2).count
    //for each common edge add (minimum edge weight/maximum edge weight)/maximum graph size to a sum
    val vSimil = (Math.min(g1.edges.map(_.attr).min, g2.edges.map(_.attr).min)/Math.max(g1.edges.map(_.attr).max, g2.edges.map(_.attr).max))/Math.max(g1EdgeCount, g2EdgeCount)*c
    vSimil
  }

  /**
   * Calculates containment similarity
   * @param g1 graph1
   * @param g2 graph2
   * @return containment similarity
   */
  private def calculateContainmentSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Double = {
    //number of edges of graph1
    val g1EdgeCount = g1.edges.distinct.count
    //number of edges of graph2
    val g2EdgeCount = g2.edges.distinct.count
    //common edges are the ones with common vertices pairs
    val srcDst1 = g1.edges.distinct.map(e => (e.srcId, e.dstId))
    val srcDst2 = g2.edges.distinct.map(e => (e.srcId, e.dstId))
    //c holds the number of common edges
    val c = srcDst1.intersection(srcDst2).count
    //for each common edge add 1/min to a sum
    val cSimil = (1.toDouble/Math.min(g1EdgeCount, g2EdgeCount))*c
    cSimil
  }

}