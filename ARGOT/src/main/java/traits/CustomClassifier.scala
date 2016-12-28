package traits

import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
trait CustomClassifier {

  val sc: SparkContext

  def train(trainset: Array[Graph[String, Double]]): Graph[String, Double]

  def test(f: String, graphs: Array[Graph[String, Double]]): Array[String]

}
