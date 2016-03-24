import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}

/**
 * @author Kontopoulos Ioannis
 * @param l the learning factor
 */
class IntersectOperator(val l: Double) extends BinaryGraphOperator with Serializable {

  /**
   * Creates a graph which contains the common edges with averaged edge weights
   * @param g1 graph1
   * @param g2 graph2
   * @return intersected graph
   */
  def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    //pair edges so the common edges are the ones with same vertices pair
    def edgeToPair (e: Edge[Double]) = ((e.srcId, e.dstId), e.attr)
    val pairs1 = g1.edges.map(edgeToPair).partitionBy(new HashPartitioner(g1.edges.getNumPartitions))
    val pairs2 = g2.edges.map(edgeToPair)
    //combine edges
    val newEdges = pairs1.join(pairs2)
      .map{ case ((srcId, dstId), (a, b)) => Edge(srcId, dstId, averageValues(a, b)) }
    //create new graph
    val intersectedGraph = Graph.fromEdges(newEdges, "intersected")
    intersectedGraph
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