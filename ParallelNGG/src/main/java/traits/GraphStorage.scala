import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait GraphStorage {

  val sc: SparkContext

  def saveGraph(g: Graph[String, Double], label: String)

  def loadGraph(label: String): Graph[String, Double]

}
