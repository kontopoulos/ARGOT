import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.classification.ClassificationModel

/**
 * @author Kontopoulos Ioannis
 */
trait ModelClassifier {

  val sc: SparkContext

  def train(classGraphs: List[Graph[String, Double]], files: Array[String]*): ClassificationModel

  def test(model: ClassificationModel, classGraphs: List[Graph[String, Double]], files: Array[String]*): Map[String, Double]

}
