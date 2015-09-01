import org.apache.spark.graphx.{Edge, Graph}

/**
 * @author Kontopoulos Ioannis
 * @param l the learning factor
 */
class GraphMerger(val l: Double) extends BinaryGraphOperator with Serializable {

  /**
   * Merges two graphs
   * @param g1 graph1
   * @param g2 graph2
   * @return merged graph
   */
  def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    //pair edges so the common edges are the ones with same vertices pair
    def edgeToPair (e: Edge[Double]) = ((e.srcId, e.dstId), e.attr)
    val pairs1 = g1.edges.map(edgeToPair)
    val pairs2 = g2.edges.map(edgeToPair)
    //combine edges
    val newEdges = pairs1.union(pairs2)
      .aggregateByKey((0.0, 0.0))(
        (acc, e) => (acc._1 + e, acc._2 + 1.0),
        (acc1, acc2) => (averageValues(acc1._1, acc2._1), averageValues(acc1._2, acc2._2))
      ).map{case ((srcId, dstId), (acc, count)) => Edge(srcId, dstId, acc)}
    //combine vertices assuming there are no conflicts like different labels
    val newVertices = g1.vertices.union(g2.vertices).distinct
    //create new graph
    val mergedGraph = Graph(newVertices, newEdges)
    mergedGraph
  }

  /**
   * Calculates the new edge weights
   * @param a weight1
   * @param b weight2
   * @return updated value
   */
  def averageValues(a: Double, b: Double): Double = {
    //updatedValue = oldValue + l × (newValue − oldValue)
    val updated =  Math.min(a,b) + l*(Math.max(a,b) - Math.min(a,b))
    updated
  }

}
