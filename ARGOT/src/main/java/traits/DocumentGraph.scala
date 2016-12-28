package traits

import java.io.IOException

/**
  * @author Kontopoulos Ioannis
  */
trait DocumentGraph {

  var edges: Map[(String,String),Double]
  var numEdges: Int

  def fromString(dataString: String): Unit

  /**
    * Merges current graph with the other graph
    * @param other graph
    * @param l learning rate
    */
  def mergeWith(other: DocumentGraph, l: Double): Unit = {
    edges = edges.toVector.union(other.edges.toVector).groupBy(_._1).mapValues{
      values =>
        // if no common edges take the first weight
        if (values.size == 1) values.map(_._2).head
        else {
          // if there are common edges, the size of the vector will always be two
          val weights = values.map(_._2)
          // first weight
          val a = weights.head
          // second weight
          val b = weights.last
          // calculate averaged weight
          averageWeights(a,b,l)
        }
    }
    numEdges = edges.size
  }

  /**
    * Intersects current graph with other graph
    * @param other graph
    * @param l learning rate
    */
  def intersectWith(other: DocumentGraph, l: Double): Unit = {
    edges = edges.filter(e => other.edges.contains(e._1))
      .map(e => (e._1,(e._2,other.edges(e._1))))
      .mapValues{case (a,b) => averageWeights(a,b,l)}
    numEdges = edges.size
  }

  /**
    * Calculates the new edge weights
    * @param a weight1
    * @param b weight2
    * @return updated value
    */
  private def averageWeights(a: Double, b: Double, l: Double): Double = {
    //updatedValue = oldValue + l × (newValue − oldValue)
    val updated =  Math.min(a,b) + l*(Math.max(a,b) - Math.min(a,b))
    updated
  }

  /**
    * Creates a graph from specified file
    * @param document file
    */
  def fromFile(document: String): Unit = {
    try {
      fromString(scala.io.Source.fromFile(document).mkString)
    }
    catch {
      case e: IOException => println(s"File not found: $document")
      case ex: Exception => println(ex.getMessage)
    }
  }

  /**
    * Creates a graph from edges
    * @param newEdges
    */
  def fromEdges(newEdges: Map[(String,String),Double]): Unit = {
    edges = newEdges
    numEdges = edges.size
  }

  /**
    * Saves graph to dot format file
    * @param fileName
    */
  def saveToDotFormat(fileName: String): Unit = {
    val writer = new java.io.FileWriter(s"$fileName.dot")
    writer.write("digraph nGramGraph {\n")
    val formattedEdges = edges.map{
      case (vertices,weight) =>
        s"\t${vertices._1.replaceAll("[ !@#$%^&*()_+-={}|,.<>/?:;']", "_")} -> ${vertices._2.replaceAll("[ !@#$%^&*()_+-={}|,.<>/?:;']", "_")} [label=$weight];"
    }.mkString("\n")
    writer.write(formattedEdges)
    writer.write("\n}")
    writer.close
  }

}