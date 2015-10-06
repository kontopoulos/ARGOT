import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel, ClassificationModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Support Vector Machines with Stochastic Gradient Descent Classifier
 * @author Kontopoulos Ioannis
 */
class SVMwithSGDSimilarityClassifier(val sc: SparkContext, val numPartitions: Int) extends ModelClassifier {

  /**
   * Creates Support Vector Machines with Stochastic Gradient Descent Model based on labeled points from training sets
   * Each labeled point consists of a label and a feature vector
   * @param classGraphs list of graphs containing the class graphs
   * @param files array containing files of the training set
   * @return training model
   */
  override def train(classGraphs: List[Graph[String, Double]], files: Array[String]*): SVMModel = {
    //labelsAndFeatures holds the labeled points for the training model
    var labelsAndFeatures = Array.empty[LabeledPoint]
    //create proper instances for graph creation and similarity calculation
    val nggc = new NGramGraphCreator(sc, numPartitions, 3, 3)
    val gsc = new GraphSimilarityCalculator
    //create labeled points from first category
    files(0).foreach{ f =>
      val e = new StringEntity
      e.readDataStringFromFile(f)
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(0.0, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized"))))
    }
    //create labeled points from second category
    files(1).foreach{ f =>
      val e = new StringEntity
      e.readDataStringFromFile(f)
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(1.0, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized"))))
    }
    val parallelLabeledPoints = sc.parallelize(labelsAndFeatures, numPartitions)
    //run training algorithm to build the model
    val numIterations = 100
    val model = SVMWithSGD.train(parallelLabeledPoints, numIterations)
    model
  }

  /**
   * Creates labeled points from testing sets and test them with the model provided
   * @param model classification model
   * @param classGraphs list of graphs containing the class graphs
   * @param files array containing files of the training set
   * @return map with evaluation metrics
   */
  override def test(model: ClassificationModel, classGraphs: List[Graph[String, Double]], files: Array[String]*): Map[String, Double] = {
    val trainedModel = model.asInstanceOf[SVMModel]
    //labelsAndFeatures holds the labeled points from the testing set
    var labelsAndFeatures = Array.empty[LabeledPoint]
    //create proper instances for graph creation and similarity calculation
    val nggc = new NGramGraphCreator(sc, numPartitions, 3, 3)
    val gsc = new GraphSimilarityCalculator
    //create labeled points from first category
    files(0).foreach{ f =>
      val e = new StringEntity
      e.readDataStringFromFile(f)
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(0.0, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized"))))
    }
    //create labeled points from second category
    files(1).foreach{ f =>
      val e = new StringEntity
      e.readDataStringFromFile(f)
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(1.0, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized"))))
    }
    val test = sc.parallelize(labelsAndFeatures, numPartitions)
    //compute raw scores on the test set.
    val predictionAndLabels = test.map(point => (trainedModel.predict(point.features), point.label))
    //get evaluation metrics.
    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    val recall = metrics.recall
    val accuracy = 1.0 * predictionAndLabels.filter(x => x._1 == x._2).count() / test.count()
    val fmeasure = metrics.fMeasure
    val values = Map("precision" -> precision, "recall" -> recall, "accuracy" -> accuracy, "fmeasure" -> fmeasure)
    values
  }

}
