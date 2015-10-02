import org.apache.spark.graphx.{Edge, Graph}

/**
 * @author Kontopoulos Ioannis
 */
class GraphSimilarityCalculator extends SimilarityCalculator with Serializable {

  /**
   * Gets the similarity between two graphs
   * @param g1 graph1
   * @param g2 graph2
   * @return Similarity
   */
  override def getSimilarity(g1: Graph[String, Double], g2: Graph[String, Double]): Similarity = {
    //store distinct edges after materializing for future use
    g1.edges.distinct.cache
    g2.edges.distinct.cache
    val gs = new GraphSimilarity(calculateSizeSimilarity(g1, g2), calculateValueSimilarity(g1, g2), calculateContainmentSimilarity(g1, g2))
    //free memory of stored edges
    g1.edges.distinct.unpersist()
    g2.edges.distinct.unpersist()
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
    //pair edges so the common edges are the ones with same vertices pair
    def edgeToPair (e: Edge[Double]) = ((e.srcId, e.dstId), e.attr)
    val pairs1 = g1.edges.map(edgeToPair)
    val pairs2 = g2.edges.map(edgeToPair)
    val commonEdges = pairs1.join(pairs2).cache
    //minimum edge weight of the common edges
    val minEdgeWeight = commonEdges.map(e => Math.min(e._2._1, e._2._2)).min
    //maximum edge weight of the common edges
    val maxEdgeWeight = commonEdges.map(e => Math.max(e._2._1, e._2._2)).max
    //c holds the number of common edges
    val c = commonEdges.count
    //for each common edge add (minimum edge weight/maximum edge weight)/maximum graph size to a sum
    val vSimil = minEdgeWeight/maxEdgeWeight/Math.max(g1EdgeCount, g2EdgeCount)*c
    //free memory
    commonEdges.unpersist()
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