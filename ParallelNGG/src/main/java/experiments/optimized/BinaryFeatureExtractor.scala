package experiments.optimized

import java.io.FileWriter

import classification.NaiveBayesClassifier
import graph.similarity.DiffSizeGSCalculator
import graph.{CachedDistributedNGramGraph, NGramGraph}
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph
import org.apache.spark.mllib.classification.{ClassificationModel, NaiveBayesModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class BinaryFeatureExtractor(sc: SparkContext, numPartitions: Int) {

  def train(classGraphs: Array[CachedDistributedNGramGraph], graphs: Array[(Double,NGramGraph)]) = {
    val gsc = new DiffSizeGSCalculator(sc)
    val fStart = System.currentTimeMillis
    //create labeled points from train graphs
    val labelsAndFeatures = graphs.map{
      case (index,g) =>
        val gs1 = gsc.getSimilarity(g, classGraphs.head)
        val gs2 = gsc.getSimilarity(g, classGraphs(1))
        //vector space consists of value, containment and normalized value similarity
        LabeledPoint(index, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized")))
    }
    val fEnd = System.currentTimeMillis
    val fTime = fEnd - fStart

    val f = new FileWriter("experiment.log",true)
    f.write("Threads " + numPartitions + " | Features training " + fTime + "\n")
    f.close

    val parallelLabeledPoints = sc.parallelize(labelsAndFeatures, numPartitions)
    //run training algorithm to build the model
    val algorithm = new NaiveBayesClassifier
    val trainStart = System.currentTimeMillis
    val model = algorithm.train(parallelLabeledPoints)
    val trainEnd = System.currentTimeMillis
    val trainTime = trainEnd - trainStart
    val t = new FileWriter("experiment.log",true)
    t.write("Threads " + numPartitions + " | MLTrain " + trainTime + "\n")
    t.close
    model
  }

  def test(model: ClassificationModel, classGraphs: Array[CachedDistributedNGramGraph], graphs: Array[(Double,NGramGraph)]) = {
    val trainedModel = model.asInstanceOf[NaiveBayesModel]
    val gsc = new DiffSizeGSCalculator(sc)
    //create labeled points from test graphs
    val labelsAndFeatures = graphs.map{
      case (index,g) =>
        val gs1 = gsc.getSimilarity(g, classGraphs.head)
        val gs2 = gsc.getSimilarity(g, classGraphs(1))
        //vector space consists of value, containment and normalized value similarity
        LabeledPoint(index, Vectors.dense(gs1.getSimilarityComponents("containment"), gs2.getSimilarityComponents("containment"), gs1.getSimilarityComponents("value"), gs2.getSimilarityComponents("value"), gs1.getSimilarityComponents("normalized"), gs2.getSimilarityComponents("normalized")))
    }
    val test = sc.parallelize(labelsAndFeatures, numPartitions)
    //compute raw scores on the test set.
    val algorithm = new NaiveBayesClassifier
    algorithm.test(trainedModel, test)
  }

}
