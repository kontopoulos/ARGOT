/**
  * @author Kontopoulos Ioannis
  */
class GraphSimilarityComparator extends SimilarityComparator {

  /**
    * Gets the similarity between two graphs
    * @param g1 first graph
    * @param g2 second graph
    * @return similarity of graphs
    */
  override def getSimilarity(g1: NGramGraph, g2: NGramGraph): Similarity = {
    //number of edges of graph1
    val g1EdgeCount = g1.numEdges
    //number of edges of graph2
    val g2EdgeCount = g2.numEdges
    //calculate size similarity
    val sSimil = Math.min(g1EdgeCount, g2EdgeCount).toDouble/Math.max(g1EdgeCount, g2EdgeCount)
    // get the edges hash
    val g1EdgesHash = g1.edges.map(_._1)
    val g2EdgesHash = g2.edges.map(_._1)
    // get the common edges
    val commonEdgesHash = g1EdgesHash.intersect(g2EdgesHash)
    // convert to hash maps
    val g1Weghts = g1.edges.toMap
    val g2Weghts = g2.edges.toMap
    // get the edge weights of both graphs for the common edges
    val commonEdges = commonEdgesHash.map(e => (e,(g1Weghts(e),g2Weghts(e))))
    // number of common edges
    val commonEdgesCount = commonEdges.length
    var vSimil = 0.0
    //if there are common edges
    if (commonEdgesCount != 0) {
      vSimil = commonEdges.map(e => Math.min(e._2._1, e._2._2)/Math.max(e._2._1, e._2._2)).sum/Math.max(g1EdgeCount,g2EdgeCount)
    }
    //for each common edge add 1/min to a sum
    val cSimil = (1.toDouble/Math.min(g1EdgeCount, g2EdgeCount))*commonEdgesCount
    val gs = new GraphSimilarity(sSimil, vSimil, cSimil)
    gs
  }

}
