import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}

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
    val numPartitions = g1.edges.partitions.size
    //pair edges to only the vertices pair
    def edgeToPair (e: Edge[Double]) = ((e.srcId, e.dstId))
    //pair edges so the common edges are the ones with same vertices pair
    def edgeAttrToPair (e: Edge[Double]) = ((e.srcId, e.dstId), e.attr)
    val pairs1 = g1.edges.map(edgeToPair)
    val pairs2 = g2.edges.map(edgeToPair)
    //get the edges from first graph that do not exist in the second graph
    val emptyEdges1 = pairs1.subtract(pairs2)
      .map{ case (srcId, dstId) => ((srcId, dstId),  0.0) }
    //get the edges from second graph that do not exist in the first graph
    val emptyEdges2 = pairs2.subtract(pairs1)
      .map{ case (srcId, dstId) => ((srcId, dstId),  0.0) }
    //get edges with their attributes
    val valuedRDD1 = g1.edges.map(edgeAttrToPair).partitionBy(new HashPartitioner(numPartitions))
    val valuedRDD2 = g2.edges.map(edgeAttrToPair).partitionBy(new HashPartitioner(numPartitions))
    //get the common edges between them
    val edges1 = valuedRDD1.join(emptyEdges1)
      .map{ case ((srcId, dstId), (a, b)) => Edge(srcId, dstId, a) }
    val edges2 = valuedRDD2.join(emptyEdges2)
      .map{ case ((srcId, dstId), (a, b)) => Edge(srcId, dstId, a) }
    //union edges
    val edges = edges1.union(edges2)
    val inverseGraph = Graph(g1.vertices.union(g2.vertices).distinct, edges)
    inverseGraph
  }

}