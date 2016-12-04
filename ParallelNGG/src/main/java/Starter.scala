import java.io.FileWriter

import clustering.MatrixMCL
import experiments.optimized.BinaryNFoldCrossValidation
import graph.operators._
import graph.similarity.{GraphSimilarityCalculator, GraphSimilarityComparator}
import nlp.{OpenNLPSentenceSplitter, StringEntityTokenizer}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author Kontopoulos Ioannis
 */
object Starter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("ParallelNGG")
      //.setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer.max","1792m")
      .registerKryoClasses(Array(classOf[MergeOperator], classOf[IntersectOperator], classOf[InverseIntersectOperator], classOf[DeltaOperator], classOf[GraphSimilarityCalculator], classOf[StringEntityTokenizer], classOf[OpenNLPSentenceSplitter], classOf[MatrixMCL], classOf[NGGMergeOperator], classOf[GraphSimilarityComparator]))
      .set("spark.executor.memory", "48g")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val numPartitions = args.head.toInt


    /*val documents = new java.io.File("MERGE/").listFiles.map( f => f.getAbsolutePath)
    val t = new NGGMergeOperator(sc,3,3,numPartitions)
    println(t.getGraph(documents).edges.count)
    t.getGraph(documents).edges.foreach(println)*/


    try {
      //val exp = new nFoldCrossValidation(sc, numPartitions, 10)
      val start = System.currentTimeMillis

      val exp = new BinaryNFoldCrossValidation(sc,numPartitions,10)
      exp.run("NaiveBayes")
      //exp.run("NaiveBayes")

      //val documents = new java.io.File("MERGE/").listFiles.map( f => f.getAbsolutePath)
      //val mo = new NGGMergeOperator(sc,3,3,numPartitions)
      //mo.getGraph(sc.parallelize(documents,numPartitions)).edges.count

      /*val mo = new MultiGraphMergeOperator(sc,numPartitions)
      val nggc = new NGramGraphCreator(sc,3,3)

      val graphs = new java.io.File("MERGE/").listFiles.map( f => f.getAbsolutePath).toSeq.map{
        f =>
          val e = new StringEntity
          e.fromFile(f)
          nggc.getGraph(e,numPartitions)
      }

      mo.getResult(graphs).edges.count*/

      val end = System.currentTimeMillis

      val time = end - start
      val w = new FileWriter("experiment.log",true)
      w.write("Threads " + numPartitions + " | Total Time " + time + "\n")
      w.close
    }
    catch {
      case e: Exception =>
        val w = new FileWriter("errors.log",true)
        w.write(e.getMessage + "\n" + e.getStackTrace.mkString("\n"))
        w.close
    }





    /*val sum = new NGGSummarizer(sc, numPartitions)
    val summaries = sum.getSummary("corpora")

    sum.saveSummaries(summaries)*/


  }

}
