package gr.demokritos.iit.nGramGraphMethods

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/**
 * @author Kontopoulos Ioannis
 */
object Main {
  def main(args: Array[String]) {
    //tests
    val conf = new SparkConf().setAppName("Graph Methods").setMaster("local")
    val sc = new SparkContext(conf)


    //===========================
    val today = Calendar.getInstance().getTime()
    val minuteFormat = new SimpleDateFormat("mm")
    val secondFormat = new SimpleDateFormat("ss")
    val millis = new SimpleDateFormat("ms")
    val currentMilli = millis.format(today)
    val currentSecond = secondFormat.format(today)
    val currentMinute = minuteFormat.format(today)
    //===========================




    val e = new StringEntity
    e.readDataStringFromFile("text1.txt")
    //e.dataString = "Hello World!"
    val nggc = new NGramGraphController(sc)
    val ngg = nggc.getGraph(e, 3, 3)

    val en = new StringEntity
    en.readDataStringFromFile("text2.txt")
    //en.dataString = "Hello Planet."
    val ngg2 = nggc.getGraph(en, 3, 3)


    //Use of Merger
    //println("====Merge Graphs====")
    //val m = new GraphMerger(0.5)
    //val g1 = m.getResult(ngg, ngg2)
    //g1.edges.distinct.collect.foreach(println)
    //println(g1.edges.distinct.count)
    //g1.vertices.collect.foreach(println)


    //Use of Intersector
    //println("====Intersect Graphs====")
    //val i = new GraphIntersector(0.5)
    //val g2 = i.getResult(ngg, ngg2)
    //g2.edges.collect.foreach(println)
    //println(g2.edges.distinct.count)
    //g2.vertices.collect.foreach(println)

    //Use of Inverse Intersector
    //println("====Inverse Intersect Graphs====")
    //val ii = new GraphInverseIntersector
    //val g3 = ii.getResult(ngg, ngg2)
    //g3.edges.collect.foreach(println)
    //println(g3.edges.distinct.count)
    //g3.vertices.collect.foreach(println)

    //Use of delta operator
    //println("====Delta Operator upon Graphs====")
    //val op = new GraphDeltaOperator
    //val g4 = op.getResult(ngg, ngg2)
    //g4.edges.collect.foreach(println)
    //println(g4.edges.distinct.count)
    //g4.vertices.collect.foreach(println)

    //Use of similarities
    val gsc = new GraphSimilarityCalculator(sc)
    val gs = gsc.getSimilarity(ngg, ngg2)
    println("Overall " + gs.getOverallSimilarity + " Size " + gs.getSimilarityComponents("size") + " Value " + gs.getSimilarityComponents("value") + " Containment " + gs.getSimilarityComponents("containment") + " Normalized " + gs.getSimilarityComponents("normalized"))

    //============================================
    val tonight = Calendar.getInstance().getTime()
    val minuteFormat2 = new SimpleDateFormat("mm")
    val secondFormat2 = new SimpleDateFormat("ss")
    val millis2 = new SimpleDateFormat("ms")
    val currentMilli2 = millis2.format(tonight)
    val currentSecond2 = secondFormat2.format(tonight)
    val currentMinute2 = minuteFormat2.format(tonight)
    println
    println("Start Time: " + currentMinute + ":" + currentSecond + ":" + currentMilli)
    println("End Time: " + currentMinute2 + ":" + currentSecond2 + ":" + currentMilli2)
    //dummy method for application runtime, better check start time and end time
    println("Application Runtime: " + (currentMinute2.toInt-currentMinute.toInt) + " mins " + (currentSecond2.toInt-currentSecond.toInt) + " secs " + (currentMilli2.toInt-currentMilli.toInt) + " millisecs")

  }

}