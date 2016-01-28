import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait CustomClassifier {

  val sc: SparkContext

  def train(trainset: Array[String]): Graph[String, Double]

  def test(f: String, graphs: List[Graph[String, Double]]): List[String]

}
