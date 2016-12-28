package ml.classification

import graph.similarity.DiffSizeGSCalculator
import graph.{DistributedCachedNGramGraph, NGramGraph}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import traits.FeatureExtractor

/**
  * @author Kontopoulos Ioannis
  */
class NGramGraphFeatureExtractor(sc: SparkContext, numPartitions: Int) extends FeatureExtractor {

  /**
    * Creates features from class graphs and document graphs
    * and returns RDD of labeled points
    * @param classGraphs array of class graphs
    * @param documents iterator of files
    * @return labeled points
    */
  override def extract(classGraphs: Array[DistributedCachedNGramGraph], documents: Array[(Double,String)]): RDD[LabeledPoint] = {
    val gsc = new DiffSizeGSCalculator(sc)
    // count the number of classes
    val numClasses = classGraphs.length
    //create labeled points from graphs
    val labelsAndFeatures = documents.map{
      case (index,fileName) =>
        val g = new NGramGraph(3,3)
        g.fromFile(fileName)
        // array which holds the features
        val featureRow = Array.fill(numClasses*3)(0.0)
        var currentClassIndex = 0
        // extract similarities for each class graph
        classGraphs.foreach{
          cg =>
            // get similarity between current graph and current class graph
            val gs = gsc.getSimilarity(g,cg)
            // extract features with 3 similarities for each class
            featureRow(currentClassIndex) = gs.getSimilarityComponents("containment")
            featureRow(currentClassIndex+numClasses) = gs.getSimilarityComponents("value")
            featureRow(currentClassIndex+2*numClasses) = gs.getSimilarityComponents("normalized")
            // increase current class index
            currentClassIndex += 1
        }
        // create corresponding labeled point with sparse feature vector
        LabeledPoint(index, Vectors.sparse(featureRow.length,(0 until featureRow.length).toArray,featureRow))
    }
    // parallelize the labeled points
    sc.parallelize(labelsAndFeatures, numPartitions)
  }

}
