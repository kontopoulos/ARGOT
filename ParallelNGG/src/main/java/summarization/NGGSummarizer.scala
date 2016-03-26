import org.apache.spark.{HashPartitioner, SparkContext}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.distributed.{IndexedRow, IndexedRowMatrix}
import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class NGGSummarizer(val sc: SparkContext, numPartitions: Int) {

  //TODO, not finished yet
  def getSummary(directory: String): Unit = {

    //val documents = new java.io.File(directory).listFiles.map(f => f.getAbsolutePath)

    val dec = new DocumentEventClustering(sc)
    //val test = dec.getClusters(documents)
    val eventClusters = dec.loadClustersFromCsv("clusters.csv")

    val ss = new OpenNLPSentenceSplitter("en-sent.bin")

    eventClusters.foreach{case (clusterId,docs) =>

      var sentences: RDD[StringAtom] = sc.parallelize(Seq(new StringAtom(0L, "dummy")), numPartitions)

      docs.foreach{d =>
        val e = new StringEntity
        e.readFile(sc, d, numPartitions)
        val s = ss.getSentences(e).asInstanceOf[RDD[StringAtom]]
        sentences = sentences.union(s)
      }

      val indexedSentences = sentences.filter(sa => sa.dataStream != "dummy").zipWithIndex
      val sMatrix = getSimilarityMatrix(indexedSentences)

      val mcl = new MatrixMCL(10,2,2.0,0.05)
      val markovClusters = mcl.getMarkovClusters(sMatrix).partitionBy(new HashPartitioner(numPartitions))

      val sentenceClusters = markovClusters.join(indexedSentences.map(s => (s._2, s._1))).map(x => x._2)



      /*println("====== clusters start ======")

      sentenceClusters.groupBy(_._1).mapValues(_.map(_._2)).filter{case (key,value) => value.nonEmpty}.foreach{ case (key,value) =>
          println("clusterId " + key)
          value.foreach(sa => println(sa.dataStream))
          println
      }

        println("====== clusters end ======")*/


    }
  }

  /**
    * Creates a similarity matrix between sentences
    * based on the Normalized Value Similarity
    * @param indexedAtoms StringAtoms with an arbitrary matrixId
    * @return similarity matrix
    */
  private def getSimilarityMatrix(indexedAtoms: RDD[(StringAtom, Long)]): IndexedRowMatrix = {
    val idxSentenceArray = indexedAtoms.collect
    //number of sentences
    val numSentences = idxSentenceArray.length

    //add self loops to matrix
    val selfLoops = indexedAtoms.map{case (a,id) => (id.toInt,(id.toInt,1.0))}

    val nggc = new NGramGraphCreator(3,3)
    val gsc = new GraphSimilarityCalculator

    var similarities = Array.empty[(Int,(Int,Double))]
    var next = 1
    //compare all sentences between them and create similarity matrix
    idxSentenceArray.foreach{ case (a,id) =>
      val curE = new StringEntity
      curE.fromString(sc, a.dataStream, numPartitions)
      val curG = nggc.getGraph(curE)
      for (i <- next to numSentences-1) {
        val e = new StringEntity
        e.fromString(sc, idxSentenceArray(i)._1.dataStream, numPartitions)
        val g = nggc.getGraph(e)
        val gs = gsc.getSimilarity(g, curG)
        similarities ++= Array((id.toInt,(idxSentenceArray(i)._2.toInt,gs.getSimilarityComponents("normalized"))))
      }
      next += 1
    }
    //convert to indexed row matrix
    val indexedRows = sc.parallelize(similarities, numPartitions).union(selfLoops)
      .groupByKey
      .map(e => IndexedRow(e._1, Vectors.sparse(numSentences, e._2.toSeq)))
    new IndexedRowMatrix(indexedRows)
  }

}
