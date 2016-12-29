package ml.classification

import org.apache.spark.mllib.classification.{ClassificationModel, SVMModel, SVMWithSGD}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import traits.ModelClassifier

/**
  * @author Kontopoulos Ioannis
  */
class SVMBinaryClassifier extends ModelClassifier {

  /**
    * Trains Support Vector Machines
    * with Stochastic Gradient Descent algorithm
    * Supports only binary classification
    * @param trainset labeled points to train the algorithm
    * @return the trained model
    */
  override def train(trainset: RDD[LabeledPoint]): SVMModel = {
    val numIterations = 100
    SVMWithSGD.train(trainset, numIterations)
  }

  /**
    * Classifies test set based on classification model
    * @param model trained model
    * @param testSet labeled points to classify
    * @return accuracy
    */
  override def test(model: ClassificationModel, testSet: RDD[LabeledPoint]): Double = {
    val trainedModel = model.asInstanceOf[SVMModel]
    //compute raw scores on the test set.
    val predictionAndLabels = testSet.map(point => (trainedModel.predict(point.features), point.label))
    val accuracy = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / testSet.count()
    accuracy
  }

}
