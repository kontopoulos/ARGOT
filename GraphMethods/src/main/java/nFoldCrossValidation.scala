import org.apache.spark.SparkContext

/**
 * @author Kontopoulos Ioannis
 */
class nFoldCrossValidation(val sc: SparkContext, val numFold: Int) extends Experiment {

  /**
   * Run the n-fold cross validation experiment
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
    var precision = 0.0
    for (j <- 0 to numFold-1) {
      precision += foldValidation(j, ens1, ens2, ens3)
    }
    precision = precision/numFold
    println("===================================")
    println("Averaged precision = " + precision)
    println("===================================")
  }

  /**
   * Calculates the precision of a fold
   * @param currentFold the number of current fold to validate
   * @param ens1 list of entities of first class
   * @param ens2 list of entities of second class
   * @param ens3 list of entities of third class
   * @return precision of current fold
   */
  def foldValidation(currentFold: Int, ens1 : List[StringEntity], ens2 : List[StringEntity], ens3 : List[StringEntity]): Double = {
    println("Separating training and testing datasets...")
    //get training and testing datasets from first category
    val entities1 = ens1.iterator.sliding(ens1.length/numFold, ens1.length/numFold)
    var training1: List[StringEntity] = Nil
    var testing1: List[StringEntity] = Nil
    var i = 0
    entities1.foreach { l =>
      if (i == currentFold) {
        testing1 :::= l.toList
      }
      else {
        training1 :::= l.toList
      }
      i += 1
    }
    //get training and testing datasets from second category
    val entities2 = ens2.iterator.sliding(ens2.length/numFold, ens2.length/numFold)
    var training2: List[StringEntity] = Nil
    var testing2: List[StringEntity] = Nil
    var j = 0
    entities2.foreach { l =>
      if (j == currentFold) {
        testing2 :::= l.toList
      }
      else {
        training2 :::= l.toList
      }
      j += 1
    }
    //get training and testing datasets from third category
    val entities3 = ens3.iterator.sliding(ens3.length/numFold, ens3.length/numFold)
    var training3: List[StringEntity] = Nil
    var testing3: List[StringEntity] = Nil
    var x = 0
    entities3.foreach { l =>
      if (x == currentFold) {
        testing3 :::= l.toList
      }
      else {
        training3 :::= l.toList
      }
      x += 1
    }
    println("Separation complete.")
    println("Training...")
    //start training upon datasets
    val cls = new NGramGraphSimilarityClassifier(sc)
    val g01 = cls.train(training1)
    val g02 = cls.train(training2)
    val g03 = cls.train(training3)
    println("Training complete.")
    //start testing datasets
    var tp01 = 0
    var fp01 = 0
    var tp02 = 0
    var fp02 = 0
    var tp03 = 0
    var fp03 = 0
    testing1.foreach{ e =>
      val label = cls.test(e, List(g01, g02, g03))
      if (label(0) == "C01") {
        //true positive for C01
        tp01 += 1
      }
      else if (label(0) == "C02") {
        //false positive for C02
        fp02 += 1
      }
      else {
        //false positive for C03
        fp03 += 1
      }
    }
    testing2.foreach{ e =>
      val label = cls.test(e, List(g01, g02, g03))
      if (label(0) == "C02") {
        //true positive for C02
        tp02 += 1
      }
      else if (label(0) == "C01") {
        //false positive for C01
        fp01 += 1
      }
      else {
        //false positive for C03
        fp03 += 1
      }
    }
    testing3.foreach{ e =>
      val label = cls.test(e, List(g01, g02, g03))
      if (label(0) == "C03") {
        //true positive for C03
        tp03 += 1
      }
      else if (label(0) == "C01") {
        //false positive for C01
        fp01 += 1
      }
      else {
        //false positive for C02
        fp02 += 1
      }
    }
    var precision1 = 0.0
    if ((tp01 + fp01) != 0) {
      precision1 = tp01.toDouble/(tp01 + fp01)
    }
    var precision2 = 0.0
    if ((tp02 + fp02) != 0) {
      precision2 = tp02.toDouble/(tp02 + fp02)
    }
    var precision3 = 0.0
    if ((tp03 + fp03) != 0) {
      precision3 = tp03.toDouble/(tp03 + fp03)
    }
    val precision = (precision1 + precision2 + precision3)/3
    println("tp01 = " + tp01 + " fp01 = " + fp01 + " tp02 = " + tp02 + " fp02 = " + fp02 + " tp03 = " + tp03 + " fp03 = " + fp03)
    println("Precision1 = " + precision1)
    println("Precision2 = " + precision2)
    println("Precision3 = " + precision3)
    println("===================================")
    println("Macro-average Precision = " + precision)
    println("===================================")
    precision
  }

}
