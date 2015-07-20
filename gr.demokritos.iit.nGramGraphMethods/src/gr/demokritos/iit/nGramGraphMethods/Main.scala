package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
object Main extends App {
  override def main(args: Array[String]) {
    println("Hello- World")
    //tests
    val conf = new SparkConf().setAppName("Graph Methods").setMaster("local")
    val sc = new SparkContext(conf)
    val vertexArray = Array(
      (1L, ("a", 28)),
      (2L, ("b", 27)),
      (3L, ("c", 65))
    )
    val edgeArray = Array(
      Edge(1L, 2L, 1),
      Edge(2L, 3L, 8)
    )
    val vertexRDD: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray)
    val edgeRDD: RDD[Edge[Int]] = sc.parallelize(edgeArray)
    val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
    val vertexArray2 = Array(
      (1L, ("a", 28)),
      (2L, ("b", 27)),
      (3L, ("c", 28)),
      (4L, ("d", 27)),
      (5L, ("e", 65))
    )
    val edgeArray2 = Array(
      Edge(1L, 2L, 1),
      Edge(2L, 3L, 4),
      Edge(3L, 5L, 1),
      Edge(2L, 4L, 1)
    )
    val vertexRDD2: RDD[(Long, (String, Int))] = sc.parallelize(vertexArray2)
    val edgeRDD2: RDD[Edge[Int]] = sc.parallelize(edgeArray2)
    val graph2: Graph[(String, Int), Int] = Graph(vertexRDD2, edgeRDD2)
    var gsc = new GraphSimilarityCalculator()
    //var gs = gsc.getSimilarity(graph, graph2)
    println(gsc.calculateValueSimilarity(graph, graph2))
  }
  
  //def converter(str: String): Long = {
    
  //}
}