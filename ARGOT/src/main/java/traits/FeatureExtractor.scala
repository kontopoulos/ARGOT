package traits

import graph.DistributedCachedNGramGraph
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
trait FeatureExtractor {

  def extract(classGraphs: Array[DistributedCachedNGramGraph], documents: Array[(Double,String)]): RDD[LabeledPoint]

}
