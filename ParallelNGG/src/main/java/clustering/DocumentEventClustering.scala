import java.io.FileWriter

import org.apache.spark.SparkContext

/**
  * @author Kontopoulos Ioannis
  */
class DocumentEventClustering(sc: SparkContext, numPartitions: Int) extends Clustering {

  /**
    * Clusters documents based on
    * the events the documents talk about
    * @param documents the documents to cluster
    * @return map containing the clusters
    */
  override def getClusters(documents: Array[String]): Map[Int, Array[String]] = {

    val gsc = new GraphSimilarityCalculator
    val wggc = new WordNGramGraphCreator(2, 3)

    //array that holds the clusters
    var clusters = Array.empty[(Int, String)]
    //initialize the cluster id
    var clusterId = 0
    //index for the next document
    var next = 1
    //compare all possible pairs of documents
    documents.foreach{doc =>
      //get the number of occurrences of this document in the cluster
      var clustered = clusters.filter(x => x._2 == doc)
      //if it is not already in the cluster, cluster it
      if (clustered.isEmpty) {
        //increase cluster id
        clusterId += 1
        clusters ++= Array((clusterId, doc))
        //create entity and graph
        val curE = new StringEntity
        curE.fromFile(sc, doc, numPartitions)
        val curG = wggc.getGraph(curE)
        //cache edges for future use
        curG.cache
        //compare current document with all the next ones
        for (i <- next to documents.length-1) {
          //get the number of occurrences of the next document in the cluster
          clustered = clusters.filter(x => x._2 == documents(i))
          //if already clustered do not compare
          if (clustered.isEmpty) {
            val e = new StringEntity
            e.fromFile(sc, documents(i), numPartitions)
            val g = wggc.getGraph(e)
            val gs = gsc.getSimilarity(curG, g)
            //if similarity values exceed a specific value add to cluster
            if (gs.getSimilarityComponents("normalized") > 0.2 && gs.getSimilarityComponents("size") > 0.1) {
              clusters ++= Array((clusterId, documents(i)))
            }
          }
        }
        //unpersist edges from memory
        curG.unpersist()
        //increase the next document index
        next += 1
      }
    }
    clusters.groupBy(_._1).mapValues(_.map(_._2))
  }

  /**
    * Save clusters to csv file format
    * @param clusters clusters to save
    */
  def saveClustersToCsv(clusters: Map[Int, Array[String]]) = {
    val w = new FileWriter("event_clusters.csv")
    try {
      clusters.foreach{case(k,v) =>
          v.foreach(el => w.write(k + "," + el + "\n"))
      }
    }
    catch {
      case ex: Exception => println("Could not write to file. Reason: " + ex.getMessage)
    }
    finally w.close
  }

  /**
    * Read clusters from csv file
    * @param file file to read
    * @return map of clusters
    */
  def loadClustersFromCsv(file: String): Map[Int,Array[String]] = {
    var clusters: Map[Int,Array[String]] = Map()
    sc.textFile(file, numPartitions).collect.foreach{line =>
      val parts = line.split(",")
      val clusterId = parts.head.toInt
      val text = parts.last
      if (clusters.contains(clusterId)) {
        val elements = clusters(clusterId) ++ Array(text)
        clusters += clusterId -> elements
      }
      else {
        clusters += clusterId -> Array(text)
      }
    }
    clusters
  }

}
