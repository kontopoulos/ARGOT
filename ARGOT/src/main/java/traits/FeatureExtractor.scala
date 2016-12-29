package traits

import graph.DistributedCachedNGramGraph
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
trait FeatureExtractor {

  def extract(classGraphs: Array[DistributedCachedNGramGraph], documents: Array[(Double,String)]): RDD[LabeledPoint]

  /**
    * Reads csv file and extracts the labeled points
    * @param sc Spark Context
    * @param numPartitions number of partitions
    * @param file to read
    * @return labeled points
    */
  def extractFromCsv(sc: SparkContext, numPartitions: Int, file: String): RDD[LabeledPoint] = {
    sc.textFile("features.csv",numPartitions).map{
      line =>
        val parts = line.split(" ")
        val index = parts.head.toDouble
        val featureRow = parts.drop(1).map(_.toDouble)
        LabeledPoint(index, Vectors.sparse(featureRow.length,(0 until featureRow.length).toArray,featureRow))
    }
  }

}
