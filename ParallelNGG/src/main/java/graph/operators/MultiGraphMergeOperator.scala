import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.graphx.{Edge, Graph}

/**
  * @author Kontopoulos Ioannis
  */
class MultiGraphMergeOperator(sc: SparkContext, numPartitions: Int) extends NaryOperator {

  /**
    * Merges multiple graphs into one
    * with default learning rate = 0.5
    * @param graphs collection of graphs to merge
    * @return one merged graph
    */
  def getResult(graphs: Seq[Graph[String, Double]]): Graph[String, Double] = {
    // union all edges of all graphs
    val mergedEdges = sc.union(
      graphs.map(_.edges.map(e => ((e.srcId,e.dstId),(e.attr,1))))
    )// place each key with same hash on the same node
      .partitionBy(new HashPartitioner(numPartitions))
      // add edge weights and count number of edges combined
      .reduceByKey{case(a,b) => (a._1+b._1,a._2+b._2)}
      // for each value calculate the average
      .mapValues{ case (sum, count) => sum / count }
      // map to graph edges
      .map(e => Edge(e._1._1,e._1._2,e._2))
    // create graph from merged edges
    Graph.fromEdges(mergedEdges,"merged")
  }

}
