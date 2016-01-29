import org.apache.spark.mllib.classification.{ClassificationModel, NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class NaiveBayesClassifier extends ModelClassifier {

  /**
    * Trains Naive Bayes algorithm
    * @param trainset labeled points to train the algorithm
    * @return the trained model
    */
  override def train(trainset: RDD[LabeledPoint]): NaiveBayesModel = {
    NaiveBayes.train(trainset)
  }

  /**
    * Classifies test set based on classification model
    * F-measure is only returned because recall equals
    * to precision for multiclass classifier because sum
    * of all false positives is equal to sum of all false negatives
    * and f-measure equals to precision and recall because precision equals recall
    * @param model trained model
    * @param testset labeled points to classify
    * @return f-measure
    */
  override def test(model: ClassificationModel, testset: RDD[LabeledPoint]): Double = {
    val trainedModel = model.asInstanceOf[NaiveBayesModel]
    //compute raw scores on the test set.
    val predictionAndLabels = testset.map(point => (trainedModel.predict(point.features), point.label))
    //get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    metrics.fMeasure
  }

}
