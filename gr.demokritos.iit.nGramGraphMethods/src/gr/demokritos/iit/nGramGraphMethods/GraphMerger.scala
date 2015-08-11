package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 * @param l the learning factor
 * @param sc SparkContext
 */
class GraphMerger(val l: Double, val sc: SparkContext) extends BinaryGraphOperator {

  /**
   * Merges two graphs
   * @param g1 graph1
   * @param g2 graph2
   * @return merged graph
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
    //search for the common edges
    m.keys.foreach { i => if (m2.contains(i)) map += (i -> m(i)) else map += (i -> new Tuple3(0L, 0L, 0.0)) }
    //search for the common edges
    m2.keys.foreach { i => if (m.contains(i)) map += (i -> m2(i)) else map += (i -> new Tuple3(0L, 0L, 0.0)) }
    //mMap holds all of the edges with the averaged weights
    var mMap: Map[String, Tuple3[Long, Long, Double]] = Map()
    if (m.size > m2.size) {
      mMap ++= calculateNewWeights(map, m, m2)
    }
    else {
      mMap ++= calculateNewWeights(map, m2, m)
    }
    //vertices holds all of the vertices
    var vertices = Array.empty[Tuple2[Long, String]]
    //combine the vertices from the two graphs
    g1.vertices.collect.foreach{
      v =>
        if(!vertices.contains((v._1, v._2))) {
          vertices = vertices ++ Array(Tuple2(v._1, v._2))
        }
    }
    g2.vertices.collect.foreach{
      v =>
        if(!vertices.contains((v._1, v._2))) {
          vertices = vertices ++ Array(Tuple2(v._1, v._2))
        }
    }
    //edges holds all of the edges
    var edges = Array.empty[Edge[Double]]
    //combine the edges from the two graphs
    g1.edges.collect.foreach{
      e =>
        if(!edges.contains(Edge(e.srcId, e.dstId, e.attr))) {
          edges = edges ++ Array(Edge(e.srcId, e.dstId, e.attr))
        }
    }
    g2.edges.collect.foreach{
      e =>
        if(!edges.contains(Edge(e.srcId, e.dstId, e.attr))) {
          edges = edges ++ Array(Edge(e.srcId, e.dstId, e.attr))
        }
    }
    //create vertex RDD from vertices array
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertices)
    //create edge RDD from edges array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edges)
    //create graph
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    //assign proper edge weights
    val mergedGraph = graph.mapEdges(e => e.attr * 0 + mMap(e.srcId + "," + e.dstId)._3)
    mergedGraph
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