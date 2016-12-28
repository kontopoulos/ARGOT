package graph.operators

import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}
import traits.BinaryGraphOperator

/**
 * @author Kontopoulos Ioannis
 */
class InverseIntersectOperator extends BinaryGraphOperator with Serializable {

  /**
   * Creates a graph that contains the uncommon edges, the edges could be from any graph
   * @param g1 graph1
   * @param g2 graph2
   * @return graph
   */
  override def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    val numPartitions = g1.edges.getNumPartitions
    //pair edges to only the vertices pair
    val pairs1 = g1.edges.map(e => (e.srcId, e.dstId))
    val pairs2 = g2.edges.map(e => (e.srcId, e.dstId))
    //get the edges from first graph that do not exist in the second graph
    val emptyEdges1 = pairs1.subtract(pairs2)
      .map{ case (srcId, dstId) => ((srcId, dstId),  0.0) }
    //get the edges from second graph that do not exist in the first graph
    val emptyEdges2 = pairs2.subtract(pairs1)
      .map{ case (srcId, dstId) => ((srcId, dstId),  0.0) }
    //pair edges so the common edges are the ones with same vertices pair
    val valuedRDD1 = g1.edges.map(e => ((e.srcId, e.dstId), e.attr)).partitionBy(new HashPartitioner(numPartitions))
    val valuedRDD2 = g2.edges.map(e => ((e.srcId, e.dstId), e.attr)).partitionBy(new HashPartitioner(numPartitions))
    //get the common edges between them
    val edges1 = valuedRDD1.join(emptyEdges1)
      .map{ case ((srcId, dstId), (a, b)) => Edge(srcId, dstId, a) }
    val edges2 = valuedRDD2.join(emptyEdges2)
      .map{ case ((srcId, dstId), (a, b)) => Edge(srcId, dstId, a) }
    //union edges
    val edges = edges1.union(edges2)
    val inverseGraph = Graph.fromEdges(edges, "inverse")
    inverseGraph
  }

}