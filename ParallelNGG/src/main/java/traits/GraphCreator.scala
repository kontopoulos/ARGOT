package traits

import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait GraphCreator {

  def getGraph(e: Entity, numPartitions: Int): Graph[String, Double]

}
