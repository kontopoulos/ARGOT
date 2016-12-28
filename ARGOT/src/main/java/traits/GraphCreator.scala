package traits

import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait GraphCreator {

  def getGraph(entity: Entity): Graph[String, Double]

}
