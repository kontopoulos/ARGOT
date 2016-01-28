import java.io.FileWriter

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class DocumentEventClustering(sc: SparkContext) extends Clustering {

  /**
    * Clusters documents based on
    * the events the documents talk about
    * @param path the directory of the documents to cluster
    * @return array containing the clusters
    */
  override def getClusters(path: String): Array[(Int, String)] = {
    val documents = new java.io.File(path).listFiles.map(f => f.getAbsolutePath)

    val gsc = new GraphSimilarityCalculator
    val wggc = new WordNGramGraphCreator(sc, 2, 2, 3)

    //array that holds the clusters
    var clusters = Array.empty[(Int, String)]
    //initialize the cluster id
    var clusterId = 0
    //index for the next document
    var next = 1
    //compare all possible pairs of documents
    documents.foreach{doc =>
      //get the number of occurrences of this document in the cluster
      var clustered = clusters.filter(x => x._2 == doc).length
      //if it is not already in the cluster, cluster it
      if (clustered == 0) {
        //increase cluster id
        clusterId += 1
        clusters = clusters ++ Array((clusterId, doc))
        //create entity and graph
        val curE = new StringEntity
        curE.readFile(sc, doc, 2)
        val curG = wggc.getGraph(curE)
        //cache edges for future use
        curG.edges.distinct.cache
        //compare current document with all the next ones
        for (i <- next to documents.length-1) {
          //get the number of occurrences of the next document in the cluster
          clustered = clusters.filter(x => x._2 == documents(i)).length
          //if already clustered do not compare
          if (clustered == 0) {
            val e = new StringEntity
            e.readFile(sc, documents(i), 2)
            val g = wggc.getGraph(e)
            val gs = gsc.getSimilarity(curG, g)
            //if similarity values exceed a specific value add to cluster
            if (gs.getSimilarityComponents("normalized") > 0.2 && gs.getSimilarityComponents("size") > 0.1) {
              clusters = clusters ++ Array((clusterId, documents(i)))
            }
          }
        }
        //unpersist edges from memory
        curG.edges.distinct.unpersist()
        //increase the next document index
        next += 1
      }
    }
    clusters
  }

  /**
    * Save clusters to csv file format
    * @param clusters clusters to save
    */
  def saveClustersToCsv(clusters: Array[(Int, String)]) = {
    val w = new FileWriter("clusters.csv")
    try {
      clusters.foreach(cl => w.write(cl._1 + "," + cl._2 + "\n"))
    }
    catch {
      case ex: Exception => {
        println("Could not write to file. Reason: " + ex.getMessage)
      }
    }
    w.close
  }

  /**
    * Read clusters from csv file
    * @param sc SparkContext
    * @param file file to read
    * @return array of clusters
    */
  def loadClustersFromCsv(sc: SparkContext, file: String): RDD[(Int, String)] = {
    sc.textFile(file).map{line =>
      val clusters = line.split(",")
      (clusters(0).toInt, clusters(1))
    }
  }

}
