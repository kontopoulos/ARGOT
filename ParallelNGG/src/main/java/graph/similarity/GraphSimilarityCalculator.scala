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
    //number of edges of graph1
    val g1EdgeCount = g1.edges.distinct.count
    //number of edges of graph2
    val g2EdgeCount = g2.edges.distinct.count
    //calculate size similarity
    val sSimil = Math.min(g1EdgeCount, g2EdgeCount).toDouble/Math.max(g1EdgeCount, g2EdgeCount)
    //pair edges so the common edges are the ones with same vertices pair
    def edgeToPair (e: Edge[Double]) = ((e.srcId, e.dstId), e.attr)
    val pairs1 = g1.edges.map(edgeToPair)
    val pairs2 = g2.edges.map(edgeToPair)
    val commonEdges = pairs1.join(pairs2)
    commonEdges.cache
    //c holds the number of common edges
    val c = commonEdges.count
    var minEdgeWeight = 1.0
    var maxEdgeWeight = 1.0
    //if there are common edges
    if (c != 0) {
      //minimum edge weight of the common edges
      minEdgeWeight = commonEdges.map(e => Math.min(e._2._1, e._2._2)).min
      //maximum edge weight of the common edges
      maxEdgeWeight = commonEdges.map(e => Math.max(e._2._1, e._2._2)).max
    }
    commonEdges.unpersist()
    //for each common edge add (minimum edge weight/maximum edge weight)/maximum graph size to a sum
    val vSimil = minEdgeWeight/maxEdgeWeight/Math.max(g1EdgeCount, g2EdgeCount)*c
    //for each common edge add 1/min to a sum
    val cSimil = (1.toDouble/Math.min(g1EdgeCount, g2EdgeCount))*c
    val gs = new GraphSimilarity(sSimil, vSimil, cSimil)
    gs
  }

}