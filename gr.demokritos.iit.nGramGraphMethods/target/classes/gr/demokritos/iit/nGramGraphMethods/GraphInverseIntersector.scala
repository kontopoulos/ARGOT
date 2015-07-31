package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
class GraphInverseIntersector extends BinaryGraphOperator {

  /**
   * Creates a graph that contains the uncommon edges, the edges could be from any graph
   * @param g1 graph1
   * @param g2 graph2
   * @return graph
   */
  override def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    //m holds the edges from the first graph
    var m:Map[String, Tuple2[Long, Long]] = Map()
    //collect edges from the first graph
    g1.edges.collect.foreach{ e => m+=(e.srcId + "," + e.dstId -> new Tuple2(e.srcId,e.dstId)) }
    //m2 holds the edges from the second graph
    var m2:Map[String, Tuple2[Long, Long]] = Map()
    //collect edges from the second graph
    g2.edges.collect.foreach{ e => m2+=(e.srcId + "," + e.dstId -> new Tuple2(e.srcId,e.dstId)) }
    //map holds all of the edges, with proper values of the uncommon edges
    var map:Map[String, Tuple2[Long, Long]] = Map()
    //search for the uncommon edges, if it does not exist add the key from the other in order not to have exception in the subgraph method
    m.keys.foreach{ i => if(!m2.contains(i)) map += (i -> m(i)) else map += (i -> new Tuple2(0L, 0L)) }
    //search for the uncommon edges, if it does not exist add the key from the other in order not to have exception in the subgraph method
    m2.keys.foreach{ i => if(!m.contains(i)) map += (i -> m2(i)) else map += (i -> new Tuple2(0L, 0L)) }
    //merge edges
    val edges = g1.edges.union(g2.edges)
    //merge vertices
    val vertices = g1.vertices.union(g2.vertices)
    //create merged graph
    val newG: Graph[String, Double] = Graph(vertices, edges)
    //merge multiple edges between two vertices into a single edge.
    val mergedGraph = newG.groupEdges((a, b) => a+b)
    //subgraph the merged graph with the uncommon edges
    val inverseIntersectedGraph = mergedGraph.subgraph(epred = e => e.srcId == map(e.srcId + "," + e.dstId)._1 && e.dstId == map(e.srcId + "," + e.dstId)._2)
    inverseIntersectedGraph
  }

}