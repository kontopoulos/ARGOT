package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 * @param sc SparkContext
 */
class GraphInverseIntersector(val sc: SparkContext) extends BinaryGraphOperator {

  /**
   * Creates a graph that contains the uncommon edges, the edges could be from any graph
   * @param g1 graph1
   * @param g2 graph2
   * @return graph
   */
  override def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    //m holds the edges from the first graph
    var m:Map[String, Tuple3[Long, Long, Double]] = Map()
    //collect edges from the first graph
    g1.edges.collect.foreach{ e => m+=(e.srcId + "," + e.dstId -> new Tuple3(e.srcId,e.dstId,e.attr)) }
    //m2 holds the edges from the second graph
    var m2:Map[String, Tuple3[Long, Long, Double]] = Map()
    //collect edges from the second graph
    g2.edges.collect.foreach{ e => m2+=(e.srcId + "," + e.dstId -> new Tuple3(e.srcId,e.dstId,e.attr)) }
    //map holds the uncommon edges
    var map:Map[String, Tuple3[Long, Long, Double]] = Map()
    //search for the uncommon edges
    m.keys.foreach{ i => if(!m2.contains(i)) map += (i -> m(i)) }
    //search for the uncommon edges
    m2.keys.foreach{ i => if(!m.contains(i)) map += (i -> m2(i)) }
    //array that holds the uncommon edges
    var edges = Array.empty[Edge[Double]]
    map.keys.foreach{
      k =>
        edges = edges ++ Array(Edge(map(k)._1, map(k)._2, map(k)._3))
    }
    //vs holds all of the vertices
    var vs:Map[Long, String] = Map()
    //collect vertices of first graph
    g1.vertices.collect.foreach{
      v =>
        vs += (v._1 -> v._2)
    }
    //collect vertices of second graph
    g2.vertices.collect.foreach{
      v =>
        vs += (v._1 -> v._2)
    }
    //vertices holds the proper vertices for inverse intersected graph
    var vertices = Array.empty[Tuple2[Long, String]]
    edges.foreach{
      e =>
        if(!vertices.contains((e.srcId, vs(e.srcId))))
          vertices = vertices ++ Array((e.srcId, vs(e.srcId)))
        if(!vertices.contains((e.dstId, vs(e.dstId))))
          vertices = vertices ++ Array((e.dstId, vs(e.dstId)))
    }
    //create vertex RDD from vertices array
    val vertexRDD: RDD[(Long, String)] = sc.parallelize(vertices)
    //create edge RDD from edges array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edges)
    //create graph
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    graph
  }

}