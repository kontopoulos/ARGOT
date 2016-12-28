package graph

import org.apache.spark.graphx.{Edge, Graph, PartitionStrategy}
import org.apache.spark.rdd.RDD
import structs.DocumentAtom
import traits.{Entity, GraphCreator}

/**
 * @author Kontopoulos Ioannis
 */
class NGramGraphCreator(dwin: Int, numPartitions: Int) extends GraphCreator with Serializable {

  /**
   * Creates a distributed graph from entity based on dwin.
   * The resulting edges might not be 100% accurate since we
   * will miss a few from the beginning and the end of each
   * partition. Given that each partition can contain several million
   * vertices, the loss in assurance should be negligible.
   * The main benefit here is that each partition can be executed in parallel.
   * @param entity graph will be created from current entity
   * @return distributed n-gram graph
   */
  override def getGraph(entity: Entity): Graph[String, Double] = {
    val atoms = entity.getAtoms(numPartitions)
      .asInstanceOf[RDD[DocumentAtom]]
      .map(a => (a.label,a.dataStream))
    // vertices of the graph
    val vertexRDD = atoms.distinct
    // create edges of the graph per partition
    val edgeRDD = atoms.mapPartitions{
      atomIterator =>
        val vertices = atomIterator.toArray
        val edges = (vertices ++ Array.fill(dwin)((-1L, null))) // add dummy vertices at the end
          .sliding(dwin + 1) // slide over dwin + 1 vertices at the time
          .flatMap(arr => {
          val (srcId, _) = arr.head //take first
          // generate 2n edges
          arr.tail.flatMap{case (dstId, _) =>
            Array(Edge(srcId, dstId, 1.0), Edge(dstId, srcId, 1.0))
          }}.filter(e => e.srcId != -1L & e.dstId != -1L)) //drop dummies
        edges
    }
    Graph(vertexRDD, edgeRDD)
      .partitionBy(PartitionStrategy.EdgePartition2D)
      // group and sum the weight of common edges
      .groupEdges((a, b) => a + b)
  }

}
