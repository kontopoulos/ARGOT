import java.io.FileWriter

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD

/**
 * @author Kontopoulos Ioannis
 */
class NGramGraphStorage(val sc: SparkContext, val numPartitions: Int) extends GraphStorage {

  /**
   * Save vertices ans edges of graph to files
   * @param g graph to save
   * @param label label of saved graph
   */
  override def saveGraph(g: Graph[String, Double], label: String) = {
    //collect edges per partition, so there is no memory overflow
    val ew = new FileWriter(label + "edges.txt")
    val edgeParts = g.edges.distinct.partitions
    for (p <- edgeParts) {
      val idx = p.index
      //The second argument is true to avoid rdd reshuffling
      val partRdd = g.edges.distinct
        .mapPartitionsWithIndex((index: Int, it: Iterator[Edge[Double]]) => if(index == idx) it else Iterator(), true )
      //partRdd contains all values from a single partition
      partRdd.collect.foreach{ e =>
        try {
          ew.write(e.srcId + "<>" + e.dstId + "<>" + e.attr + "\n")
        }
        catch {
          case ex: Exception => {
            println("Could not write to file. Reason: " + ex.getMessage)
          }
        }
      }
    }
    //close file
    ew.close
    //collect vertices per partition, so there is no memory overflow
    val vw = new FileWriter(label + "vertices.txt")
    val vertexParts = g.vertices.distinct.partitions
    for (p <- vertexParts) {
      val idx = p.index
      //The second argument is true to avoid rdd reshuffling
      val partRdd = g.vertices.distinct
        .mapPartitionsWithIndex((index: Int, it: Iterator[(Long, String)]) => if (index == idx) it else Iterator(), true )
      //partRdd contains all values from a single partition
      partRdd.collect.foreach{ v =>
        try {
          vw.write(v._1 + "<>" + v._2 + "\n")
        }
        catch {
          case ex: Exception => {
            println("Could not write to file. Reason: " + ex.getMessage)
          }
        }
      }
    }
    //close file
    vw.close
  }

  /**
   * Load graph from edges file and vertices file
   * @param label label of saved graph
   * @return graph
   */
  override def loadGraph(label: String): Graph[String, Double] = {
    //path for vertices file
    val vertexFile = label + "vertices.txt"
    //path for edges file
    val edgeFile = label + "edges.txt"
    //create EdgeRDD from file rows
    val edges: RDD[Edge[Double]] = sc.textFile(edgeFile, numPartitions).map{ line =>
      val row = line.split("<>")
      Edge(row(0).toLong, row(1).toLong, row(2).toDouble)
    }
    //create VertexRDD from file rows
    val vertices: RDD[(Long, String)] = sc.textFile(vertexFile, numPartitions).map{ line =>
      val row = line.split("<>")
      (row(0).toLong, row(1))
    }
    //create graph
    val graph: Graph[String, Double] = Graph(vertices, edges)
    graph
  }

  /**
   * Saves graph to dot format file
   * @param g graph to save
   */
  def saveGraphToDotFormat(g: Graph[String, Double]) = {
    val w = new FileWriter("nGramGraph.dot")
    try {
      g.vertices.cache
      w.write("digraph nGramGraph {\n")
      val edgeParts = g.edges.distinct.partitions
      for (p <- edgeParts) {
        val idx = p.index
        //The second argument is true to avoid rdd reshuffling
        val partRdd = g.edges.distinct
          .mapPartitionsWithIndex((index: Int, it: Iterator[Edge[Double]]) => if(index == idx) it else Iterator(), true )
        //partRdd contains all values from a single partition
        partRdd.collect.foreach{ e =>
          w.write("\t" + g.vertices.filter{ v => v._1 == e.srcId}.first._2.replaceAll("[ !@#$%^&*()_+-={}|,.<>/?:;']", "_") + " -> " + g.vertices.filter{ v => v._1 == e.dstId}.first._2.replaceAll("[ !@#$%^&*()_+-={}|,.<>/?:;']", "_") + " [label=\"" + e.srcId + "" + e.dstId + "\" weight=" + e.attr + "];\n")
        }
      }
      w.write("}")
      g.vertices.unpersist()
    }
    catch {
      case ex: Exception => {
        println("Could not write to file. Reason: " + ex.getMessage)
      }
    }
    w.close
  }

}
