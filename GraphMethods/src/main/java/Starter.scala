import org.apache.spark.{SparkContext, SparkConf}

/**
 * @author Kontopoulos Ioannis
 */
object Starter {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Graph Methods")
      //.setMaster("local")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryoserializer.buffer","24mb")
      .registerKryoClasses(Array(classOf[MergeOperator], classOf[IntersectOperator], classOf[InverseIntersectOperator], classOf[DeltaOperator]))
      .set("spark.executor.memory", "16g")
    val sc = new SparkContext(conf)

    val exp = new nFoldCrossValidation(sc, 10)
    exp.run(args(0))



    //Examples of basic use

    //val e = new StringEntity
    //e.readDataStringFromFile("text1.txt")
    //e.dataString = "Hello World!"
    //val nggc = new NGramGraphCreator(sc, 3, 3)
    //val ngg = nggc.getGraph(e)

    //val en = new StringEntity
    //en.readDataStringFromFile("text2.txt")
    //en.dataString = "Hello Planet."
    //val ngg2 = nggc.getGraph(en)

    //Use of Merge Operator
    //println("====Merge Graphs====")
    //val mo = new MergeOperator(0.5)
    //val g1 = mo.getResult(ngg, ngg2)
    //g1.edges.distinct.collect.foreach(println)
    //println(g1.edges.distinct.count)
    //g1.vertices.collect.foreach(println)

    //Use of Intersect Operator
    //println("====Intersect Graphs====")
    //val io = new IntersectOperator(0.5)
    //val g2 = io.getResult(ngg, ngg2)
    //g2.edges.collect.foreach(println)
    //println(g2.edges.distinct.count)
    //g2.vertices.collect.foreach(println)

    //Use of Inverse Intersect Operator
    //println("====Inverse Intersect Graphs====")
    //val ii = new InverseIntersectOperator
    //val g3 = ii.getResult(ngg, ngg2)
    //g3.edges.collect.foreach(println)
    //println(g3.edges.distinct.count)
    //g3.vertices.collect.foreach(println)

    //Use of delta operator
    //println("====Delta Operator upon Graphs====")
    //val do = new DeltaOperator
    //val g4 = do.getResult(ngg, ngg2)
    //g4.edges.collect.foreach(println)
    //println(g4.vertices.distinct.count)
    //g4.vertices.collect.foreach(println)

    //Use of similarities
    //val gsc = new GraphSimilarityCalculator
    //val gs = gsc.getSimilarity(ngg, ngg2)
    //println("Overall " + gs.getOverallSimilarity + " Size " + gs.getSimilarityComponents("size") + " Value " + gs.getSimilarityComponents("value") + " Containment " + gs.getSimilarityComponents("containment") + " Normalized " + gs.getSimilarityComponents("normalized"))
  }

}
