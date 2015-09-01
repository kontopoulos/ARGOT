import org.apache.spark.graphx.Graph
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by Kontopoulos Ioannis
 */
object Starter {
  def main(args: Array[String]) {
    //tests
    val conf = new SparkConf().setAppName("Graph Methods")
    val sc = new SparkContext(conf)


    //read files from directory
    var graphs: List[Graph[String, Double]] = List()
    val nggc = new NGramGraphController(sc)
    println("Reading training files...")
    new java.io.File("cardiovascular/training/C01/").listFiles.foreach{ f =>
      val e = new StringEntity
      e.readDataStringFromFile("cardiovascular/training/C01/" + f.getName)
      val c = nggc.getGraph(e, 3, 3)
      graphs :::= List(c)
    }
    println("Reading complete.")
    //merge all graphs to a class graph
    val m = new GraphMerger(0.5)
    var merged: Graph[String, Double] = null
    println("Merging graphs...")
    merged = m.getResult(graphs(0), graphs(1))
    for (i <- 2 to graphs.size-1) {
      merged = m.getResult(merged, graphs(i))
    }
    println("Merging complete.")

    val test = new StringEntity
    test.readDataStringFromFile("cardiovascular/test/C01/0000011")
    val tg = nggc.getGraph(test, 3, 3)

    //val e = new StringEntity
    //e.readDataStringFromFile("text1.txt")
    //e.dataString = "Hello World!"
    //val nggc = new NGramGraphController(sc)
    //val ngg = nggc.getGraph(e, 3, 3)

    //val en = new StringEntity
    //en.readDataStringFromFile("text2.txt")
    //en.dataString = "Hello Planet."
    //val ngg2 = nggc.getGraph(en, 3, 3)


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
    val gsc = new GraphSimilarityCalculator
    val gs = gsc.getSimilarity(tg, merged)
    println("Overall " + gs.getOverallSimilarity + " Size " + gs.getSimilarityComponents("size") + " Value " + gs.getSimilarityComponents("value") + " Containment " + gs.getSimilarityComponents("containment") + " Normalized " + gs.getSimilarityComponents("normalized"))

  }
}
