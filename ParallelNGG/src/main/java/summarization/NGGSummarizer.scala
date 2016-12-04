package summarization

import java.io.FileWriter

import clustering.{DocumentEventClustering, MatrixMCL}
import gr.demokritos.iit.jinsect.documentModel.comparators.NGramCachedGraphComparator
import gr.demokritos.iit.jinsect.documentModel.representations.DocumentNGramSymWinGraph
import graph.NGramGraphCreator
import graph.operators.{MultiGraphIntersectOperator, MultiGraphMergeOperator}
import graph.similarity.GraphSimilarityCalculator
import nlp.OpenNLPSentenceSplitter
import org.apache.spark.graphx.Graph
import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD
import structs.{DistributedStringEntity, StringAtom, StringEntity}
import traits.MultiDocumentSummarizer

/**
  * @author Kontopoulos Ioannis
  */
class NGGSummarizer(val sc: SparkContext, val numPartitions: Int) extends MultiDocumentSummarizer {

  /**
    * Clusters multiple documents and creates a summary per cluster
    * @param directory directory to extract summaries from
    * @return map containing summaries per cluster
    */
  override def getSummary(directory: String): Map[Int,Array[String]] = {

    val dec = new DocumentEventClustering(sc, numPartitions)
    println("Clustering documents into events...")

    //cluster multiple documents into events
    val eventClusters = dec.getClusters(new java.io.File(directory).listFiles.map(f => f.getAbsolutePath))
    dec.saveClustersToCsv(eventClusters)

    //variable that holds a summary per event
    var summaries: Map[Int,Array[String]] = Map()

    //for every event extract a summary
    eventClusters.foreach{case (clusterId,docs) =>

      val summary = getTopicSummary(docs)

      summaries += clusterId -> summary

    }
    summaries
  }

  // TODO use JInsect for all the small graph operations (intersection between sentences, all sentences are small)
  /**
    * Given an array of documents (path to documents)
    * extracts the summary of the documents
    * @param documents to summarize
    * @return array of sentences
    */
  def getTopicSummary(documents: Array[String]): Array[String] = {

    val ss = new OpenNLPSentenceSplitter("model_file.bin")

    var sentences = Array.empty[StringAtom]

    println("Extracting sentences...")
    //extract the sentences of the event
    documents.foreach{d =>
      val e = new DistributedStringEntity
      e.fromFile(sc, d, numPartitions)
      val s = ss.getSentences(e).asInstanceOf[RDD[StringAtom]]
      sentences = sentences ++ s.collect
    }

    //give each sentence an id
    val indexedSentences = sc.parallelize(sentences,numPartitions).zipWithIndex
    println("Creating sentence similarity matrix...")
    //get the similarity matrix based on normalized value similarity
    val sMatrix = getSimilarityMatrix(indexedSentences)

    //initialize markov clustering algorithm with 100 iterations,
    //expansion rate of 2, inflation rate of 2 and epsilon value of 0.05
    val mcl = new MatrixMCL(100,2,2.0,0.05)

    println("Markov Clustering on the matrix...")

    //get the sentence clusters
    val markovClusters = mcl.getMarkovClusters(sMatrix).partitionBy(new HashPartitioner(numPartitions))
    markovClusters.cache
    val reverseIndex = indexedSentences.map(s => (s._2, s._1))

    //retrieve sentence strings based on sentence ids
    val sentenceClusters = markovClusters.join(reverseIndex).map(x => x._2)

    //intersect the graph sentences of a cluster to create subtopics
    var subtopics = Array.empty[Graph[String,Double]]
    val io = new MultiGraphIntersectOperator(numPartitions)
    val nggc = new NGramGraphCreator(sc,3,3)

    println("Extracting subtopics...")
    sentenceClusters.collect.groupBy(_._1).mapValues(_.map(_._2)).foreach{ case (key,value) =>
      val graphs = value.map{
        a =>
          val e = new StringEntity
          e.setDataString(a.dataStream)
          nggc.getGraph(e,numPartitions)
      }.toSeq
      subtopics :+= io.getResult(graphs)
    }

    println("Creating the essence of the documents...")
    //merge the subtopic graphs to create the essence of the event
    val mo = new MultiGraphMergeOperator(sc,numPartitions)

    val eventEssence = mo.getResult(subtopics)
    eventEssence.cache

    println("Comparing each sentence to the essence...")
    //compare each sentence to the merged graph
    var sentencesToFilter = Array.empty[(Double,String)]
    val dsc = new GraphSimilarityCalculator
    indexedSentences.map(_._1.dataStream).collect.foreach{s =>
      val curE = new StringEntity
      curE.setDataString(s)
      val gs = dsc.getSimilarity(nggc.getGraph(curE,numPartitions),eventEssence)
      sentencesToFilter :+= (gs.getSimilarityComponents("value"),s)
    }
    eventEssence.unpersist()

    //sort sentences based on their value similarity to the merged graph
    val sortedSentences = sc.parallelize(sentencesToFilter, numPartitions).sortByKey(false, numPartitions).map(_._2).collect

    println("Removing redundant sentences...")
    val filter = new RedundancyRemover(sc)
    //remove redundant sentences
    val summary =  filter.getFilteredSentences(sortedSentences)
    println("Done!")
    summary
  }

  /**
    * Creates a similarity matrix between sentences
    * based on the Normalized Value Similarity
    * @param indexedAtoms StringAtoms with an arbitrary matrixId
    * @return similarity matrix
    */
  private def getSimilarityMatrix(indexedAtoms: RDD[(StringAtom, Long)]): IndexedRowMatrix = {
    //number of sentences
    val numSentences = indexedAtoms.count.toInt

    //compare all possible pairs of sentences
    val similarities = indexedAtoms.cartesian(indexedAtoms)
      //remove duplicate combinations
      .filter{case (a,b) => a._2 >= b._2}
      //compare graphs of sentences
      .map{case (a,b) =>
        val ngc = new NGramCachedGraphComparator
        val g1 = new DocumentNGramSymWinGraph(3,3,3)
        g1.setDataString(a._1.dataStream)
        val g2 = new DocumentNGramSymWinGraph(3,3,3)
        g2.setDataString(b._1.dataStream)
        val gs = ngc.getSimilarityBetween(g1,g2)
        val nvs = gs.ValueSimilarity/gs.SizeSimilarity
        (a._2.toInt,(b._2.toInt,nvs))
    }

    //convert to indexed row matrix
    val indexedRows = similarities.partitionBy(new HashPartitioner(numPartitions))
      .groupByKey
      .map(e => IndexedRow(e._1, Vectors.sparse(numSentences, e._2.toSeq)))
    new IndexedRowMatrix(indexedRows)
  }

  /**
    * Save summaries to file
    * @param summaries to save
    */
  def saveSummaries(summaries: Map[Int, Array[String]]) = {
    try {
      summaries.foreach{case(k,v) =>
        val w = new FileWriter("summary_" + k + ".txt")
        v.foreach(s => w.write(s + "\n"))
        w.close
      }
    }
    catch {
      case ex: Exception => println("Could not write to file. Reason: " + ex.getMessage)
    }
  }

  /**
    * Save summary to file
    * @param summary to save
    */
  def saveSummary(summary: Array[String]) = {
    val w = new FileWriter("summary.txt")
    try {
      summary.foreach(s => w.write(s + "\n"))
    }
    catch {
      case ex: Exception => println("Could not write to file. Reason: " + ex.getMessage)
    }
    finally w.close
  }

}
