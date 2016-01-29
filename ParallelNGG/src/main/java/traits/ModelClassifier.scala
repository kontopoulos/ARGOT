import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
trait ModelClassifier {

  def train(trainset: RDD[LabeledPoint]): ClassificationModel

  def test(model: ClassificationModel, testset: RDD[LabeledPoint]): Double

}
