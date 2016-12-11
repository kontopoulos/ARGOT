package graph.similarity

import graph.DistributedCachedNGramGraph
import org.apache.spark.SparkContext
import traits.{DocumentGraph, Similarity}

/**
  * This is a special case of similarity calculator.
  * It is used when we want to compare two graphs of different size.
  * Specifically, it is used to compare a serial graph and a distributed cached one.
  * @author Kontopoulos Ioannis
  */
class DiffSizeGSCalculator(sc: SparkContext) {

  /**
    * Calculates the similarity between
    * a distributed graph and a serial graph
    * @param smallGraph serial graph
    * @param dGraph distributed graph
    * @return similarity of graphs
    */
  def getSimilarity(smallGraph: DocumentGraph, dGraph: DistributedCachedNGramGraph): Similarity = {
    // number of edges of large graph
    val largeEdgeCount = dGraph.numEdges
    // number of edges of small graph
    val smallEdgeCount = smallGraph.numEdges
    // calculate size similarity
    val sSimil = Math.min(smallEdgeCount, largeEdgeCount).toDouble/Math.max(smallEdgeCount, largeEdgeCount)
    // map edges to key/value pairs and broadcast edges of small graph to the cluster
    val smallGraphEdges = sc.broadcast(smallGraph.edges)
    // extract the common edges of the graphs
    val commonEdges = dGraph.edges
      // take edges that exist in both graphs
      .filter(e => smallGraphEdges.value.contains((e._1._1,e._1._2)))
      // now each partition is too small, so there is no need for distribution
      .collect
      // map edges to key/value pairs where the value is a tuple of both edge weights
      .map(e => ((e._1),(e._2,smallGraph.edges(e._1))))
    // the variable below holds the number of common edges
    val commonEdgesCount = commonEdges.length
    var vSimil = 0.0
    // if there are common edges
    if (commonEdgesCount != 0) {
      vSimil = commonEdges.map(e => Math.min(e._2._1, e._2._2)/Math.max(e._2._1, e._2._2)).sum/Math.max(smallEdgeCount,largeEdgeCount)
    }
    // for each common edge add 1/min to a sum
    val cSimil = (1.toDouble/Math.min(smallEdgeCount, largeEdgeCount))*commonEdgesCount
    val gs = new GraphSimilarity(sSimil, vSimil, cSimil)
    gs
  }

}
