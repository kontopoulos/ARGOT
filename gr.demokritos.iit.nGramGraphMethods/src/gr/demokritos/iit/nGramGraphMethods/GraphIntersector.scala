package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 * @param l the learning factor
 * @param sc SparkContext
 */
class GraphIntersector(val l: Double, val sc: SparkContext) extends BinaryGraphOperator {

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
    //map holds the common edges
    var map: Map[String, Tuple3[Long, Long, Double]] = Map()
    //search for the common edges
    m.keys.foreach { i => if (m2.contains(i)) map += (i -> m(i)) }
    //search for the common edges
    m2.keys.foreach { i => if (m.contains(i)) map += (i -> m2(i)) }
    //mMap holds the common edges after the averaged weights
    var mMap: Map[String, Tuple3[Long, Long, Double]] = Map()
    if (m.size > m2.size) {
    mMap ++= calculateNewWeights(map, m, m2)
    }
    else {
      mMap ++= calculateNewWeights(map, m2, m)
    }
    //v1 holds the vertices of first graph
    var v1 = Array.empty[Tuple2[Long, String]]
    //v2 holds the vertices of second graph
    var v2 = Array.empty[Tuple2[Long, String]]
    //collect vertices of first graph
    g1.vertices.collect.foreach{
      v =>
        v1 = v1 ++ Array(Tuple2(v._1, v._2))
    }
    //collect vertices of second graph
    g2.vertices.collect.foreach{
      v =>
        v2 = v2 ++ Array(Tuple2(v._1, v._2))
    }
    //vertices holds the common vertices
    var vertices = Array.empty[Tuple2[Long, String]]
    //collect the common vertices
    g1.vertices.collect.foreach{
      v =>
        if(v1.contains(v._1, v._2) && v2.contains(v._1, v._2)) {
          vertices = vertices ++ Array(Tuple2(v._1, v._2))
        }
    }
    //array that holds the common edges
    var edges = Array.empty[Edge[Double]]
    mMap.keys.foreach{
      k =>
        edges = edges ++ Array(Edge(mMap(k)._1, mMap(k)._2, mMap(k)._3))
    }
    //create vertex RDD from vertices array
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertices)
    //create edge RDD from edges array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edges)
    //create graph
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    graph
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
    }
    mMap
  }

}