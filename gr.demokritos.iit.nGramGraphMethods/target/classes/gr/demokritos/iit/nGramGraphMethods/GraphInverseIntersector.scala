package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
class GraphInverseIntersector extends BinaryGraphOperator {
  //not actual implementation
  //just basic in order not to have compile errors
  val conf = new SparkConf().setAppName("Graph Methods").setMaster("local")
  val sc = new SparkContext(conf)
  def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    val vertexArray = Array(
      (1L, ("a")),
      (2L, ("b")),
      (3L, ("c"))
    )
    val edgeArray = Array(
      Edge(1L, 2L, 1.0),
      Edge(2L, 3L, 8.0)
    )
    val vertexRDD: RDD[(Long, (String))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edgeArray)
    val graph: Graph[String, Double] = Graph(vertexRDD, edgeRDD)
    graph
  }
}