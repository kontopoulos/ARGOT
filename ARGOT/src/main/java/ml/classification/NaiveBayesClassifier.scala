package ml.classification

import org.apache.spark.mllib.classification.{ClassificationModel, NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import traits.ModelClassifier

/**
  * @author Kontopoulos Ioannis
  */
class NaiveBayesClassifier extends ModelClassifier {

  /**
    * Trains Naive Bayes algorithm
    * @param trainSet labeled points to train the algorithm
    * @return the trained model
    */
  override def train(trainSet: RDD[LabeledPoint]): NaiveBayesModel = {
    NaiveBayes.train(trainSet)
  }

  /**
    * Classifies test set based on classification model
    * @param model trained model
    * @param testSet labeled points to classify
    * @return accuracy
    */
  override def test(model: ClassificationModel, testSet: RDD[LabeledPoint]): Double = {
    val trainedModel = model.asInstanceOf[NaiveBayesModel]
    //compute raw scores on the test set.
    val predictionAndLabels = testSet.map(point => (trainedModel.predict(point.features), point.label))
    val accuracy = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / testSet.count()
    accuracy
  }

}
