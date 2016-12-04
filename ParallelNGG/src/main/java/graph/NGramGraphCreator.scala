package graph

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import structs.StringEntity
import traits.{Entity, GraphCreator}

/**
 * @author Kontopoulos Ioannis
 */
class NGramGraphCreator(sc: SparkContext, ngram: Int, val dwin: Int) extends GraphCreator {

  /**
   * Creates a graph based on ngram, dwin and entity
   * @param e entity from which a graph will be created
   * @return n-gram graph from entity
   */
  override def getGraph(e: Entity, numPartitions: Int): Graph[String, Double] = {
    val en = e.asInstanceOf[StringEntity]
    // create vertices from n-grams
    val vertices = en.getPayload.sliding(ngram).map(atom => (("_" + atom).hashCode.toLong, "_" + atom)).toArray

    val edges = (vertices ++ Array.fill(dwin)((-1L, null))) //add dummy vertices at the end
      .sliding(dwin + 1) //slide over dwin + 1 vertices at the time
      .flatMap(arr => {
      val (srcId, _) = arr.head //take first
      // generate 2n edges
      arr.tail.flatMap{case (dstId, _) =>
        Array(Edge(srcId, dstId, 1.0), Edge(dstId, srcId, 1.0))
      }}.filter(e => e.srcId != -1L & e.dstId != -1L)) //drop dummies
      .toArray
    // create vertex RDD from vertices array
    val vertexRDD: RDD[(Long,String)] = sc.parallelize(vertices,numPartitions)
    // create edge RDD from edges array
    val edgeRDD: RDD[Edge[Double]] = sc.parallelize(edges, numPartitions)
    // create graph from vertices and edges arrays, erase duplicate edges and increase occurrence
    Graph(vertexRDD.distinct, edgeRDD)
      .partitionBy(PartitionStrategy.EdgePartition2D)
      .groupEdges((a, b) => a + b)
  }

}
