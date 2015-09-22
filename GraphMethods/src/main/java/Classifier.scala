import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.classification.NaiveBayesModel

/**
 * @author Kontopoulos Ioannis
 */
trait Classifier {

  val sc: SparkContext

  def train(classGraphs: List[Graph[String, Double]], ens: List[Entity]*): Any

  def test(model: NaiveBayesModel, classGraphs: List[Graph[String, Double]], ens: List[Entity]*): Any

}
