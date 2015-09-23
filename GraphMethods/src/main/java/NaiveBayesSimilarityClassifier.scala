import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.classification.{NaiveBayesModel, NaiveBayes}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * Naive Bayes ModelClassifier
 * @author Kontopoulos Ioannis
 */
class NaiveBayesSimilarityClassifier(val sc: SparkContext) extends ModelClassifier {

  var model: NaiveBayesModel = null

  /**
   * Creates Naive Bayes Model based on labeled points from training sets
   * Each labeled point consists of a label and a feature vector
   * @param classGraphs list of graphs containing the class graphs
   * @param ens array of lists containing entities of the training set
   * @return labeled points
   */
  override def train(classGraphs: List[Graph[String, Double]], ens: List[Entity]*) = {
    val es1 = ens(0).asInstanceOf[List[StringEntity]]
    val es2 = ens(1).asInstanceOf[List[StringEntity]]
    val es3 = ens(2).asInstanceOf[List[StringEntity]]
    //labelsAndFeatures holds the labeled points for the training model
    var labelsAndFeatures = Array.empty[LabeledPoint]
    //create proper instances for graph creation and similarity calculation
    val nggc = new NGramGraphCreator(sc, 3, 3)
    val gsc = new GraphSimilarityCalculator
    //create labeled points from first category
    es1.foreach{ e =>
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      val gs3 = gsc.getSimilarity(g, classGraphs(2))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(0.0, Vectors.dense(gs1.getSimilarityComponents("value"), gs1.getSimilarityComponents("containment"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("value"), gs2.getSimilarityComponents("containment"), gs2.getSimilarityComponents("normalized"), gs3.getSimilarityComponents("value"), gs3.getSimilarityComponents("containment"), gs3.getSimilarityComponents("normalized"))))
    }
    //create labeled points from second category
    es2.foreach{ e =>
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      val gs3 = gsc.getSimilarity(g, classGraphs(2))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(1.0, Vectors.dense(gs1.getSimilarityComponents("value"), gs1.getSimilarityComponents("containment"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("value"), gs2.getSimilarityComponents("containment"), gs2.getSimilarityComponents("normalized"), gs3.getSimilarityComponents("value"), gs3.getSimilarityComponents("containment"), gs3.getSimilarityComponents("normalized"))))
    }
    //create labeled points from third category
    es3.foreach{ e =>
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      val gs3 = gsc.getSimilarity(g, classGraphs(2))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(2.0, Vectors.dense(gs1.getSimilarityComponents("value"), gs1.getSimilarityComponents("containment"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("value"), gs2.getSimilarityComponents("containment"), gs2.getSimilarityComponents("normalized"), gs3.getSimilarityComponents("value"), gs3.getSimilarityComponents("containment"), gs3.getSimilarityComponents("normalized"))))
    }
    val parallelLabeledPoints = sc.parallelize(labelsAndFeatures)
    this.model = NaiveBayes.train(parallelLabeledPoints)
  }

  /**
   * Creates labeled points from testing sets and test them with the model provided
   * @param classGraphs list of graphs containing the class graphs
   * @param ens array of lists containing entities of the testing set
   * @return map with values of precision, recall, accuracy and f-measure
   */
  override def test(classGraphs: List[Graph[String, Double]], ens: List[Entity]*): Map[String, Double] = {
    val es1 = ens(0).asInstanceOf[List[StringEntity]]
    val es2 = ens(1).asInstanceOf[List[StringEntity]]
    val es3 = ens(2).asInstanceOf[List[StringEntity]]
    //labelsAndFeatures holds the labeled points from the testing set
    var labelsAndFeatures = Array.empty[LabeledPoint]
    //create proper instances for graph creation and similarity calculation
    val nggc = new NGramGraphCreator(sc, 3, 3)
    val gsc = new GraphSimilarityCalculator
    //create labeled points from first category
    es1.foreach{ e =>
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      val gs3 = gsc.getSimilarity(g, classGraphs(2))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(0.0, Vectors.dense(gs1.getSimilarityComponents("value"), gs1.getSimilarityComponents("containment"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("value"), gs2.getSimilarityComponents("containment"), gs2.getSimilarityComponents("normalized"), gs3.getSimilarityComponents("value"), gs3.getSimilarityComponents("containment"), gs3.getSimilarityComponents("normalized"))))
    }
    //create labeled points from second category
    es2.foreach{ e =>
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      val gs3 = gsc.getSimilarity(g, classGraphs(2))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(1.0, Vectors.dense(gs1.getSimilarityComponents("value"), gs1.getSimilarityComponents("containment"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("value"), gs2.getSimilarityComponents("containment"), gs2.getSimilarityComponents("normalized"), gs3.getSimilarityComponents("value"), gs3.getSimilarityComponents("containment"), gs3.getSimilarityComponents("normalized"))))
    }
    //create labeled points from third category
    es3.foreach{ e =>
      val g = nggc.getGraph(e)
      val gs1 = gsc.getSimilarity(g, classGraphs(0))
      val gs2 = gsc.getSimilarity(g, classGraphs(1))
      val gs3 = gsc.getSimilarity(g, classGraphs(2))
      //vector space consists of value, containment and normalized value similarity
      labelsAndFeatures = labelsAndFeatures ++ Array(LabeledPoint(2.0, Vectors.dense(gs1.getSimilarityComponents("value"), gs1.getSimilarityComponents("containment"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("value"), gs2.getSimilarityComponents("containment"), gs2.getSimilarityComponents("normalized"), gs3.getSimilarityComponents("value"), gs3.getSimilarityComponents("containment"), gs3.getSimilarityComponents("normalized"))))
    }
    val test = sc.parallelize(labelsAndFeatures)
    //compute raw scores on the test set.
    val predictionAndLabels = test.map(p => (this.model.predict(p.features), p.label))
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
