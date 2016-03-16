import org.apache.spark.graphx.{PartitionStrategy, Graph}

/**
 * @author Kontopoulos Ioannis
 * @param l the learning factor
 */
class MergeOperator(val l: Double) extends BinaryGraphOperator with Serializable {

  /**
   * Merges two graphs
   * @param g1 graph1
   * @param g2 graph2
   * @return merged graph
   */
  def getResult(g1: Graph[String, Double], g2: Graph[String, Double]): Graph[String, Double] = {
    //combine vertices and edges
    val merged = Graph(g1.vertices.union(g2.vertices).distinct, g1.edges.union(g2.edges).distinct)
      //repartition so we can group edges per partition
      .partitionBy(PartitionStrategy.EdgePartition2D)
      //apply the averaging function on edges
      .groupEdges((a, b) => averageValues(a, b))
    merged
  }

  /**
   * Calculates the new edge weights
   * @param a weight1
   * @param b weight2
   * @return updated value
   */
  def averageValues(a: Double, b: Double): Double = {
    //updatedValue = oldValue + l × (newValue − oldValue)
    val updated =  Math.min(a,b) + l*(Math.max(a,b) - Math.min(a,b))
    updated
  }

}
