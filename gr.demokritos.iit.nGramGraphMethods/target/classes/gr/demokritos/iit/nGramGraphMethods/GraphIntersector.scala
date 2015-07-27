package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.graphx._

/**
 * @author Kontopoulos Ioannis
 * @param l the learning factor
 */
class GraphIntersector(val l: Double) extends BinaryGraphOperator {

  /**
   * Creates a graph which contains the common edges with averaged edge weights
   * @param g1 graph1
   * @param g2 graph2
   * @return graph
   */
  def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    //m holds the edges from the first graph
    var m: Map[String, Tuple3[Long, Long, Double]] = Map()
    //collect edges from the first graph
    g1.edges.collect.foreach { e => m += (e.srcId + "," + e.dstId -> new Tuple3(e.srcId, e.dstId, e.attr)) }
    //m2 holds the edges from the second graph
    var m2: Map[String, Tuple3[Long, Long, Double]] = Map()
    //collect edges from the second graph
    g2.edges.collect.foreach { e => m2 += (e.srcId + "," + e.dstId -> new Tuple3(e.srcId, e.dstId, e.attr)) }
    //map holds all of the edges, with real values only from common edges
    var map: Map[String, Tuple3[Long, Long, Double]] = Map()
    //search for the common edges, if it does not exist add the key from the other in order not to have exception in the subgraph method
    m.keys.foreach { i => if (m2.contains(i)) map += (i -> m(i)) else map += (i -> new Tuple3(0L, 0L, 0.0)) }
    //search for the common edges, if it does not exist add the key from the other in order not to have exception in the subgraph method
    m2.keys.foreach { i => if (m.contains(i)) map += (i -> m2(i)) else map += (i -> new Tuple3(0L, 0L, 0.0)) }
    //mMap holds all of the edges, with proper values of common edges
    var mMap: Map[String, Tuple3[Long, Long, Double]] = Map()
    if (m.size > m2.size) {
    mMap ++= calculateNewWeights(map, m, m2)
    }
    else {
      mMap ++= calculateNewWeights(map, m2, m)
    }
    val newG = g1.mask(g2)
    //assign the proper edge weights
    val n = newG.mapEdges(e => e.attr * 0 + mMap(e.srcId + "," + e.dstId)._3)
    n
  }

  /**
   * Calculates the edge weights based on the learning factor
   * @param map contains the common edges
   * @param m contains the edges of first graph
   * @param m2 contains the edges of second graph
   * @return map with proper edge weights
   */
  private def calculateNewWeights(map: Map[String, Tuple3[Long, Long, Double]], m: Map[String, Tuple3[Long, Long, Double]], m2: Map[String, Tuple3[Long, Long, Double]]): collection.mutable.Map[String, Tuple3[Long, Long, Double]] = {
    var mMap = collection.mutable.Map[String, Tuple3[Long, Long, Double]]() ++= map
    mMap.keys.foreach {
      i =>
        if (m2.contains(i) && m.contains(i)) {
          if (m(i)._3 > m2(i)._3) {
            //updatedValue = oldValue + l*(newValue - oldValue)
            val u = m2(i)._3 + l * (m(i)._3 - m2(i)._3)
            mMap += (i -> new Tuple3(m(i)._1, m(i)._2, u))
          }
          else {
            //updatedValue = oldValue + l*(newValue - oldValue)
            val u = m(i)._3 + l * (m2(i)._3 - m(i)._3)
            mMap += (i -> new Tuple3(m(i)._1, m(i)._2, u))
          }
        }
        else if(m2.contains(i)) {
          mMap += (i -> new Tuple3(m2(i)._1, m2(i)._2, m2(i)._3))
        }
        else {
          mMap += (i -> new Tuple3(m(i)._1, m(i)._2, m(i)._3))
        }
    }
    mMap
  }

}