import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
/**
  * This is a special case of similarity calculator.
  * It is recommended to be used when we compare two graphs
  * of different size. Specifically, the smallest graph
  * should be the first method parameter and the largest
  * graph should be the second parameter of the method
  * @author Kontopoulos Ioannis
  */
class DiffSizeGSCalculator(sc: SparkContext) extends SimilarityCalculator {

  /**
    * Gets the similarity between two graphs
    * @param smallGraph the small graph
    * @param largeGraph the large graph
    * @return Similarity
    */
  override def getSimilarity(smallGraph: Graph[String, Double], largeGraph: Graph[String, Double]): Similarity = {
    // number of edges of large graph
    val largeEdgeCount = largeGraph.edges.count
    // collect the edges of the small graph
    val fewEdges = smallGraph.edges.collect
    // number of edges of small graph
    val smallEdgeCount = fewEdges.length
    // calculate size similarity
    val sSimil = Math.min(smallEdgeCount, largeEdgeCount).toDouble/Math.max(smallEdgeCount, largeEdgeCount)
    // map edges to key/value pairs and broadcast edges of small graph to the cluster
    val smallGraphEdges = sc.broadcast(fewEdges.map(e => ((e.srcId, e.dstId), e.attr)).toMap)
    // map edges of the large graph to key/value pairs
    val largeGraphEdges = largeGraph.edges.map(e => ((e.srcId, e.dstId), e.attr))
    // extract the common edges of the graphs
    val commonEdges = largeGraphEdges
      // take edges that exist in both graphs
      .filter(e => smallGraphEdges.value.contains((e._1._1,e._1._2)))
      // map edges to key/value pairs where the value is a tuple of both edge weights
      .map(e => ((e._1),(e._2,smallGraphEdges.value(e._1))))
    commonEdges.cache
    // c holds the number of common edges
    val c = commonEdges.count
    var vSimil = 0.0
    // if there are common edges
    if (c != 0) {
      vSimil = commonEdges.map(e => Math.min(e._2._1, e._2._2)/Math.max(e._2._1, e._2._2)).sum/Math.max(smallEdgeCount,largeEdgeCount)
    }
    commonEdges.unpersist()
    // for each common edge add 1/min to a sum
    val cSimil = (1.toDouble/Math.min(smallEdgeCount, largeEdgeCount))*c
    val gs = new GraphSimilarity(sSimil, vSimil, cSimil)
    gs
  }

}
