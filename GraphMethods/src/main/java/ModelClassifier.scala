import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait ModelClassifier {

  val sc: SparkContext

  def train(classGraphs: List[Graph[String, Double]], ens: List[Entity]*): Any

  def test(classGraphs: List[Graph[String, Double]], ens: List[Entity]*): Any

}
