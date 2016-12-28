package graph.operators

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}
import traits.BinaryGraphOperator

/**
 * @author Kontopoulos Ioannis
 */
class DeltaOperator extends BinaryGraphOperator with Serializable {

  /**
   * Creates a graph that contains edges from
   * the first graph that do not exist in the second graph
   * @param g1 first graph
   * @param g2 second graph
   * @return graph
   */
  override def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    val numPartitions = g1.edges.getNumPartitions
    //pair edges to only the vertices pair
    val pairs1 = g1.edges.map(e => ((e.srcId, e.dstId)))
    val pairs2 = g2.edges.map(e => ((e.srcId, e.dstId)))
    //get the edges from first graph that do not exist in the second graph
    val emptyEdges = pairs1.subtract(pairs2)
      .map{ case (srcId, dstId) => ((srcId, dstId),  0.0) }
    //pair edges so the common edges are the ones with same vertices pair
    val RDDWithValues = g1.edges.map(e => ((e.srcId, e.dstId), e.attr)).partitionBy(new HashPartitioner(numPartitions))
    //get the common edges between them
    val edges = RDDWithValues.join(emptyEdges)
      .map{ case ((srcId, dstId), (a, b)) => Edge(srcId, dstId, a) }
    //create new graph
    val deltaGraph = Graph.fromEdges(edges, "delta")
    deltaGraph
  }

}