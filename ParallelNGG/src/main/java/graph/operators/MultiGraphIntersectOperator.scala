import org.apache.spark.HashPartitioner
import org.apache.spark.graphx.{Edge, Graph}

/**
  * @author Kontopoulos Ioannis
  */
class MultiGraphIntersectOperator(numPartitions: Int) extends NaryOperator {

  /**
    * Intersects multiple graphs into one
    * with default learning rate = 0.5
    * @param graphs collection of graphs to intersect
    * @return one intersected graph
    */
  override def getResult(graphs: Seq[Graph[String, Double]]): Graph[String, Double] = {
    // map graphs to their edges with additional 1 for counting
    val intersectedEdges = graphs.map(_.edges.map(e => ((e.srcId, e.dstId), (e.attr, 1))))
      // intersect graphs to each other
      .reduce(
        (x, y) =>
          // intersect two graphs
          x.join(y.partitionBy(new HashPartitioner(numPartitions)))
            // sum the edge weights, sum the counter
            .mapValues(a => (a._1._1 + a._2._1, a._1._2 + a._2._2))
      )
      // calculate the averaged edge weight
      .mapValues { case (sum, count) => sum / count }
      // map to graph edges
      .map(e => Edge(e._1._1, e._1._2, e._2))
    // create graph from intersected edges
    Graph.fromEdges(intersectedEdges, "intersected")
  }

}
