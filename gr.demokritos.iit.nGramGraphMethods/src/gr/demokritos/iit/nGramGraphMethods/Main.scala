package gr.demokritos.iit.nGramGraphMethods

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author Kontopoulos Ioannis
 */
object Main extends App {
  override def main(args: Array[String]) {
    //tests
    val conf = new SparkConf().setAppName("Graph Methods").setMaster("local")
    val sc = new SparkContext(conf)

    val e = new StringEntity
    e.readDataStringFromFile("text1.txt")
    //e.dataString = "Hello World!"
    val nggc = new NGramGraphCreator(3, 3, sc)
    val ngg = nggc.getGraph(e)

    val en = new StringEntity
    en.readDataStringFromFile("text2.txt")
    //en.dataString = "Hello Planet."
    val nggc2 = new NGramGraphCreator(3, 3, sc)
    val ngg2 = nggc2.getGraph(en)


    //Use of Merger
    //println("====Merge Graphs====")
    //val m = new GraphMerger(0.5)
    //val g1 = m.getResult(ngg, ngg2)
    //g1.edges.collect.foreach(println)
    //println(g1.numEdges)
    //g1.vertices.collect.foreach(println)

    //Use of Intersector
    //println("====Intersect Graphs====")
    //val i = new GraphIntersector(0.5)
    //val g2 = i.getResult(ngg, ngg2)
    //g2.edges.collect.foreach(println)
    //println(g2.numEdges)
    //g2.vertices.collect.foreach(println)

    //Use of Inverse Intersector
    //println("====Inverse Intersect Graphs====")
    //val ii = new GraphInverseIntersector
    //val g3 = ii.getResult(ngg, ngg2)
    //g3.edges.collect.foreach(println)
    //println(g3.numEdges)
    //g3.vertices.collect.foreach(println)

    //Use of delta operator
    //println("====Delta Operator upon Graphs====")
    //val op = new GraphDeltaOperator
    //val g4 = op.getResult(ngg, ngg2)
    //g4.edges.collect.foreach(println)
    //println(g4.numEdges)
    //g4.vertices.collect.foreach(println)

    //Use of similarities
    val gsc = new GraphSimilarityCalculator
    val gs = gsc.getSimilarity(ngg, ngg2)
    println("Overall " + gs.getOverallSimilarity + " Size " + gs.getSimilarityComponents("size") + " Value " + gs.getSimilarityComponents("value") + " Containment " + gs.getSimilarityComponents("containment"))

  }

}