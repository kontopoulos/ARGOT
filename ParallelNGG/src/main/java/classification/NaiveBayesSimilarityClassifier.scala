import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.classification.{ClassificationModel, NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Naive Bayes Classifier
 * @author Kontopoulos Ioannis
 */
class NaiveBayesSimilarityClassifier(val sc: SparkContext, val numPartitions: Int) extends ModelClassifier {

  /**
   * Creates Naive Bayes Model based on labeled points from training sets
   * Each labeled point consists of a label and a feature vector
   * @param classGraphs list of graphs containing the class graphs
   * @param files array containing files of the training set
   * @return training model
   */
  override def train(classGraphs: List[Graph[String, Double]], files: Array[String]*): NaiveBayesModel = {
    //labelsAndFeatures holds the labeled points for the training model
    var labelsAndFeatures = Array.empty[LabeledPoint]
    //create proper instances for graph creation and similarity calculation
    val nggc = new NGramGraphCreator(sc, numPartitions, 3, 3)
    val gsc = new GraphSimilarityCalculator
    //create labeled points from first category
    files(0).foreach{ f =>
      val e = new StringEntity
      e.readFile(sc, f, numPartitions)
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(0.0, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized"))))
    }
    //create labeled points from second category
    files(1).foreach{ f =>
      val e = new StringEntity
      e.readFile(sc, f, numPartitions)
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(1.0, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized"))))
    }
    val parallelLabeledPoints = sc.parallelize(labelsAndFeatures, numPartitions)
    //run training algorithm to build the model
    val model = NaiveBayes.train(parallelLabeledPoints)
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
    val trainedModel = model.asInstanceOf[NaiveBayesModel]
    //labelsAndFeatures holds the labeled points from the testing set
    var labelsAndFeatures = Array.empty[LabeledPoint]
    //create proper instances for graph creation and similarity calculation
    val nggc = new NGramGraphCreator(sc, numPartitions, 3, 3)
    val gsc = new GraphSimilarityCalculator
    //create labeled points from first category
    files(0).foreach{ f =>
      val e = new StringEntity
      e.readFile(sc, f, numPartitions)
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(0.0, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized"))))
    }
    //create labeled points from second category
    files(1).foreach{ f =>
      val e = new StringEntity
      e.readFile(sc, f, numPartitions)
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
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)
    val pr = metrics.areaUnderPR
    val roc = metrics.areaUnderROC
    val values = Map("pr" -> pr, "roc" -> roc)
    values
  }

}
