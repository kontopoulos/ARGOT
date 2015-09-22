import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
class nFoldCrossValidation(val sc: SparkContext, val numFold: Int) extends Experiment {

  /**
   * Runs the n-fold cross validation
   */
  override def run() = {
    println("Reading files...")
    var ens1 : List[StringEntity] = Nil
    var ens2 : List[StringEntity] = Nil
    var ens3 : List[StringEntity] = Nil
    //read files
    new java.io.File("C01/").listFiles.foreach{ f =>
      val e = new StringEntity
      e.readDataStringFromFile("C01/" + f.getName)
      ens1 :::= List(e)
    }
    new java.io.File("C02/").listFiles.foreach{ f =>
      val e = new StringEntity
      e.readDataStringFromFile("C02/" + f.getName)
      ens2 :::= List(e)
    }
    new java.io.File("C03/").listFiles.foreach{ f =>
      val e = new StringEntity
      e.readDataStringFromFile("C03/" + f.getName)
      ens3 :::= List(e)
    }
    println("Reading complete.")
    var preList: List[Double] = Nil
    var precision = 0.0
    var recall = 0.0
    var accuracy = 0.0
    var fmeasure = 0.0
    for (j <- 0 to numFold-1) {
      val metrics = foldValidation(j, ens1, ens2, ens3)
      precision += metrics("precision")
      preList :::= List(metrics("precision"))
      recall += metrics("recall")
      accuracy += metrics("accuracy")
      fmeasure += metrics("fmeasure")
    }
    //calculate averaged metrics
    precision = precision/numFold
    recall = recall/numFold
    accuracy = accuracy/numFold
    fmeasure = fmeasure/numFold
    //calculate standard deviation
    var sum = 0.0
    preList.foreach{ p =>
      sum += Math.pow((p-precision), 2)
    }
    val stdev = Math.sqrt(sum)
    //calculate standard error
    val sterr = stdev/(Math.sqrt(numFold))
    println("===================================")
    println("Precision = " + precision)
    println("===================================")
    println("Recall = " + recall)
    println("===================================")
    println("Accuracy = " + accuracy)
    println("===================================")
    println("F-measure = " + fmeasure)
    println("===================================")
    println("Standard Deviation of Precision = " + stdev + " Standard Error = " + sterr)
    println("===================================")
  }

  /**
   * Calculates the evaluation metrics of the current fold
   * @param currentFold the fold to be validated
   * @param ens1 entities from first category
   * @param ens2 entities from second category
   * @param ens3 entities from third category
   * @return map with evaluation metrics
   */
  def foldValidation(currentFold: Int, ens1 : List[StringEntity], ens2 : List[StringEntity], ens3 : List[StringEntity]): Map[String, Double] = {
    println("Separating training and testing datasets...")
    //get training and testing datasets from first category
    val testing1 = ens1.slice(currentFold, currentFold+ens1.size*numFold/100)
    val training1 = ens1.slice(0, currentFold) ++ ens1.slice(currentFold+ens1.size*numFold/100, ens1.size)
    //get training and testing datasets from second category
    val testing2 = ens2.slice(currentFold, currentFold+ens2.size*numFold/100)
    val training2 = ens2.slice(0, currentFold) ++ ens2.slice(currentFold+ens2.size*numFold/100, ens2.size)
    //get training and testing datasets from third category
    val testing3 = ens3.slice(currentFold, currentFold+ens3.size*numFold/100)
    val training3 = ens3.slice(0, currentFold) ++ ens3.slice(currentFold+ens3.size*numFold/100, ens3.size)
    println("Separation complete.")
    println("Preparing operations...")
    val nggc = new NGramGraphCreator(sc, 3, 3)
    var graphs1: List[Graph[String, Double]] = Nil
    var graphs2: List[Graph[String, Double]] = Nil
    var graphs3: List[Graph[String, Double]] = Nil
    //create graphs from entities
    training1.foreach{ e =>
      val g = nggc.getGraph(e)
      graphs1 :::= List(g)
    }
    training2.foreach{ e =>
      val g = nggc.getGraph(e)
      graphs2 :::= List(g)
    }
    training3.foreach{ e =>
      val g = nggc.getGraph(e)
      graphs3 :::= List(g)
    }
    val m = new MergeOperator(0.5)
    //merge graphs from first training set to a class graph
    var classGraph1 = m.getResult(graphs1(0), graphs1(1))
    for (i <- 2 to graphs1.size-1) {
      if (i % 30 == 0) {
        //every 30 iterations cut the lineage, due to long iteration
        classGraph1 = Graph(classGraph1.vertices.distinct, classGraph1.edges.distinct)
      }
      classGraph1 = m.getResult(classGraph1, graphs1(i))
    }
    //merge graphs from second training set to a class graph
    var classGraph2 = m.getResult(graphs2(0), graphs2(1))
    for (i <- 2 to graphs2.size-1) {
      if (i % 30 == 0) {
        //every 30 iterations cut the lineage, due to long iteration
        classGraph2 = Graph(classGraph2.vertices.distinct, classGraph2.edges.distinct)
      }
      classGraph2 = m.getResult(classGraph2, graphs1(i))
    }
    //merge graphs from third training set to a class graph
    var classGraph3 = m.getResult(graphs3(0), graphs3(1))
    for (i <- 2 to graphs3.size-1) {
      if (i % 30 == 0) {
        //every 30 iterations cut the lineage, due to long iteration
        classGraph3 = Graph(classGraph3.vertices.distinct, classGraph3.edges.distinct)
      }
      classGraph3 = m.getResult(classGraph3, graphs1(i))
    }
    println("Preparations complete.")
    //start training
    println("Training...")
    val cls = new NaiveBayesSimilarityClassifier(sc)
    val model = cls.train(List(classGraph1, classGraph2, classGraph3), ens1, ens2, ens3)
    println("Training complete.")
    //start testing
    println("Testing...")
    val metrics = cls.test(model, List(classGraph1, classGraph2, classGraph3), ens1, ens2, ens3)
    println("Testing complete")
    println("===========================")
    println("Fold Completed = " + (currentFold+1))
    println("===========================")
    metrics
  }

}
