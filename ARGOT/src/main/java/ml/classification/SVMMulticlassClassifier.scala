package ml.classification

import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import traits.ModelClassifier

/**
  * @author Kontopoulos Ioannis
  */
class SVMMulticlassClassifier extends ModelClassifier {

  /**
    * Trains Support Vector Machines
    * with Stochastic Gradient Descent algorithm
    * Supports multi-class classification using
    * one-vs-rest method
    * @param trainset labeled points to train the algorithm
    * @return the trained model
    */
  override def train(trainset: RDD[LabeledPoint]): SVMMultiClassOVAModel = {
    SVMMultiClassOVAWithSGD.train(trainset,1000)
  }

  /**
    * Classifies test set based on classification model
    * @param model trained model
    * @param testset labeled points to classify
    * @return accuracy
    */
  override def test(model: ClassificationModel, testset: RDD[LabeledPoint]): Double = {
    val predictionAndLabels = testset.map(point => (model.predict(point.features), point.label))
    val accuracy = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / testset.count()
    accuracy
  }
}
