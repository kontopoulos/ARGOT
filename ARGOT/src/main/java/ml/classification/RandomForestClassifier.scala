package ml.classification

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class RandomForestClassifier {

  /**
    * Trains random forest algorithm
    * @param trainset labeled points to train the algorithm
    * @return the trained model
    */
  def train(trainset: RDD[LabeledPoint]): RandomForestModel = {
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = trainset.map(_.label).distinct.count.toInt
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 100
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 10
    val maxBins = 32
    RandomForest.trainClassifier(trainset, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
  }

  /**
    * Classifies test set based on classification model
    * @param model trained model
    * @param testset labeled points to classify
    * @return
    */
  def test(model: RandomForestModel, testset: RDD[LabeledPoint]): Double = {
    // Evaluate model on test instances and compute accuracy
    val labelAndPreds = testset.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }
    val accuracy = labelAndPreds.filter(r => r._1 == r._2).count.toDouble / testset.count()
    accuracy
  }

}
