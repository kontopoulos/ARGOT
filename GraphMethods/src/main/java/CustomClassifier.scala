import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait CustomClassifier {

  val sc: SparkContext

  def train(ens: List[Entity]): Graph[String, Double]

  def test(e: Entity, graphs: List[Graph[String, Double]]): List[String]

}
