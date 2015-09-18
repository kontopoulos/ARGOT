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
    var recall = 0.0
    var accuracy = 0.0
    for (j <- 0 to numFold-1) {
      val values = foldValidation(j, ens1, ens2, ens3)
      precision += values("precision")
      recall += values("recall")
      accuracy += values("accuracy")
    }
    precision = precision/numFold
    recall = recall/numFold
    accuracy = accuracy/numFold
    val fmeasure = 2*(precision*recall)/(precision + recall)
    println("===================================")
    println("Precision = " + precision)
    println("===================================")
    println("Recall = " + recall)
    println("===================================")
    println("Accuracy = " + accuracy)
    println("===================================")
    println("F-measure = " + fmeasure)
    println("===================================")
  }

  /**
   * Calculates the precision, recall and accuracy of a fold
   * @param currentFold the number of current fold to validate
   * @param ens1 list of entities of first class
   * @param ens2 list of entities of second class
   * @param ens3 list of entities of third class
   * @return map with the corresponding values
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
    println("Training...")
    //start training upon datasets
    val cls = new NGramGraphSimilarityClassifier(sc)
    val g01 = cls.train(training1)
    val g02 = cls.train(training2)
    val g03 = cls.train(training3)
    println("Training complete.")
    //start testing datasets
    //values for first class
    var tp01 = 0
    var fp01 = 0
    var tn01 = 0
    var fn01 = 0
    //values for second class
    var tp02 = 0
    var fp02 = 0
    var tn02 = 0
    var fn02 = 0
    //values for third class
    var tp03 = 0
    var fp03 = 0
    var tn03 = 0
    var fn03 = 0
    testing1.foreach{ e =>
      val label = cls.test(e, List(g01, g02, g03))
      if (label(0) == "C01") {
        //true positive for C01
        tp01 += 1
        //true negative for C02
        tn02 += 1
        //true negative for C03
        tn03 += 1
      }
      else if (label(0) == "C02") {
        //false positive for C02
        fp02 += 1
        //false negative for C01
        fn01 += 1
      }
      else {
        //false positive for C03
        fp03 += 1
        //false negative for C01
        fn01 += 1
      }
    }
    testing2.foreach{ e =>
      val label = cls.test(e, List(g01, g02, g03))
      if (label(0) == "C02") {
        //true positive for C02
        tp02 += 1
        //true negative for C01
        tn01 += 1
        //true negative for C03
        tn03 += 1
      }
      else if (label(0) == "C01") {
        //false positive for C01
        fp01 += 1
        //false negative for C02
        fn02 += 1
      }
      else {
        //false positive for C03
        fp03 += 1
        //false negative for C02
        fn02 += 1
      }
    }
    testing3.foreach{ e =>
      val label = cls.test(e, List(g01, g02, g03))
      if (label(0) == "C03") {
        //true positive for C03
        tp03 += 1
        //true negative for C01
        tn01 += 1
        //true negative for C02
        tn02 += 1
      }
      else if (label(0) == "C01") {
        //false positive for C01
        fp01 += 1
        //false negative for C03
        fn03 += 1
      }
      else {
        //false positive for C02
        fp02 += 1
        //false negative for C03
        fn03 += 1
      }
    }
    //calculate precision
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
    //calculate recall
    var recall1 = 0.0
    if ((tp01 + fn01) != 0) {
      recall1 = tp01.toDouble/(tp01 + fn01)
    }
    var recall2 = 0.0
    if ((tp02 + fn02) != 0) {
      recall2 = tp02.toDouble/(tp02 + fn02)
    }
    var recall3 = 0.0
    if ((tp03 + fn03) != 0) {
      recall3 = tp03.toDouble/(tp03 + fn03)
    }
    //calculate accuracy
    var accuracy1 = 0.0
    if ((tp01 + fp01 + tn01 + fn01) != 0) {
      accuracy1 = (tp01 + tn01).toDouble/(tp01 + fp01 + tn01 + fn01)
    }
    var accuracy2 = 0.0
    if ((tp02 + fp02 + tn02 + fn02) != 0) {
      accuracy2 = (tp02 + tn02).toDouble/(tp02 + fp02 + tn02 + fn02)
    }
    var accuracy3 = 0.0
    if ((tp03 + fp03 + tn03 + fn03) != 0) {
      accuracy3 = (tp03 + tn03).toDouble/(tp03 + fp03 + tn03 + fn03)
    }
    //calculate averaged values
    val precision = (precision1 + precision2 + precision3)/3
    val recall = (recall1 + recall2 + recall3)/3
    val accuracy = (accuracy1 + accuracy2 + accuracy3)/3
    println("===================================")
    println("Fold Completed = " + (currentFold + 1))
    println("===================================")
    val values = Map("precision" -> precision, "recall" -> recall, "accuracy" -> accuracy)
    values
  }

}
