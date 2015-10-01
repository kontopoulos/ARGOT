import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
class nFoldCrossValidation(val sc: SparkContext, val numFold: Int) extends Experiment {

  /**
   * Runs the n-fold cross validation
   * using the selected classifier
   * @param classifier type of classifier
   */
  override def run(classifier: String) = {
    if (classifier == "NaiveBayes") {
      println("Naive Bayes selected")
      runWithNaiveBayesClassifier()
    }
    else if (classifier == "SVMwithSGD") {
      println("Support Vector Machines with Stochastic Gradient Descent selected")
      runWithSVMwithSGDClassifier()
    }
    else if (classifier == "simple") {
      println("Simple Classifier selected")
      runWithSimpleClassifier()
    }
    else {
      println("Wrong option!!!")
    }
  }

  /**
   * Runs the n-fold cross validation
   * using Naive Bayes Classifier
   */
  private def runWithNaiveBayesClassifier() = {
    println("Reading files...")
    var ens1 : List[StringEntity] = Nil
    var ens2 : List[StringEntity] = Nil
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
    println("Reading complete.")
    var preList: List[Double] = Nil
    var precision = 0.0
    var recall = 0.0
    var accuracy = 0.0
    var fmeasure = 0.0
    for (j <- 0 to numFold-1) {
      val metrics = naiveBayesFoldValidation(j, ens1, ens2)
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
    sum = sum/numFold
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
    println("Standard Deviation of Precision = " + stdev)
    println("===================================")
    println("Standard Error of Precision = " + sterr)
    println("===================================")
  }

  /**
   * Calculates the evaluation metrics of the current fold
   * @param currentFold the fold to be validated
   * @param ens1 entities from first category
   * @param ens2 entities from second category
   * @return map with evaluation metrics
   */
  private def naiveBayesFoldValidation(currentFold: Int, ens1 : List[StringEntity], ens2 : List[StringEntity]): Map[String, Double] = {
    println("Separating training and testing datasets...")
    //get training and testing datasets from first category
    val testing1 = ens1.slice(currentFold, currentFold+ens1.size*numFold/100)
    val training1 = ens1.slice(0, currentFold) ++ ens1.slice(currentFold+ens1.size*numFold/100, ens1.size)
    //get training and testing datasets from second category
    val testing2 = ens2.slice(currentFold, currentFold+ens2.size*numFold/100)
    val training2 = ens2.slice(0, currentFold) ++ ens2.slice(currentFold+ens2.size*numFold/100, ens2.size)
    println("Separation complete.")
    println("Preparing operations...")
    val nggc = new NGramGraphCreator(sc, 3, 3)
    var graphs1: List[Graph[String, Double]] = Nil
    var graphs2: List[Graph[String, Double]] = Nil
    //create graphs from entities
    training1.foreach{ e =>
      val g = nggc.getGraph(e)
      graphs1 :::= List(g)
    }
    training2.foreach{ e =>
      val g = nggc.getGraph(e)
      graphs2 :::= List(g)
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
      classGraph2 = m.getResult(classGraph2, graphs2(i))
    }
    println("Preparations complete.")
    //start training
    println("Training...")
    val cls = new NaiveBayesSimilarityClassifier(sc)
    val model = cls.train(List(classGraph1, classGraph2), training1, training2)
    println("Training complete.")
    //start testing
    println("Testing...")
    val metrics = cls.test(model, List(classGraph1, classGraph2), testing1, testing2)
    println("Testing complete")
    println("===========================")
    println("Fold Completed = " + (currentFold+1))
    println("===========================")
    metrics
  }

  /**
   * Runs the n-fold cross validation
   * using the Support Vector Machines with Stochastic Gradient Descent Similarity Classifier
   */
  private def runWithSVMwithSGDClassifier() = {
    println("Reading files...")
    var ens1 : List[StringEntity] = Nil
    var ens2 : List[StringEntity] = Nil
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
    println("Reading complete.")
    var preList: List[Double] = Nil
    var precision = 0.0
    var recall = 0.0
    var accuracy = 0.0
    var fmeasure = 0.0
    for (j <- 0 to numFold-1) {
      val metrics = svmFoldValidation(j, ens1, ens2)
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
    sum = sum/numFold
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
    println("Standard Deviation of Precision = " + stdev)
    println("===================================")
    println("Standard Error of Precision = " + sterr)
    println("===================================")
  }

  /**
   * Calculates the evaluation metrics of the current fold
   * @param currentFold the fold to be validated
   * @param ens1 entities from first category
   * @param ens2 entities from second category
   * @return map with evaluation metrics
   */
  private def svmFoldValidation(currentFold: Int, ens1 : List[StringEntity], ens2 : List[StringEntity]): Map[String, Double] = {
    println("Separating training and testing datasets...")
    //get training and testing datasets from first category
    val testing1 = ens1.slice(currentFold, currentFold+ens1.size*numFold/100)
    val training1 = ens1.slice(0, currentFold) ++ ens1.slice(currentFold+ens1.size*numFold/100, ens1.size)
    //get training and testing datasets from second category
    val testing2 = ens2.slice(currentFold, currentFold+ens2.size*numFold/100)
    val training2 = ens2.slice(0, currentFold) ++ ens2.slice(currentFold+ens2.size*numFold/100, ens2.size)
    println("Separation complete.")
    println("Preparing operations...")
    val nggc = new NGramGraphCreator(sc, 3, 3)
    var graphs1: List[Graph[String, Double]] = Nil
    var graphs2: List[Graph[String, Double]] = Nil
    //create graphs from entities
    training1.foreach{ e =>
      val g = nggc.getGraph(e)
      graphs1 :::= List(g)
    }
    training2.foreach{ e =>
      val g = nggc.getGraph(e)
      graphs2 :::= List(g)
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
      classGraph2 = m.getResult(classGraph2, graphs2(i))
    }
    println("Preparations complete.")
    //start training
    println("Training...")
    val cls = new SVMwithSGDSimilarityClassifier(sc)
    val model = cls.train(List(classGraph1, classGraph2), training1, training2)
    println("Training complete.")
    //start testing
    println("Testing...")
    val metrics = cls.test(model, List(classGraph1, classGraph2), testing1, testing2)
    println("Testing complete")
    println("===========================")
    println("Fold Completed = " + (currentFold+1))
    println("===========================")
    metrics
  }

  /**
   * Runs the n-fold cross validation
   * using the Simple Similarity Classifier
   */
  private def runWithSimpleClassifier() = {
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
    var preList: List[Double] = Nil
    for (j <- 0 to numFold-1) {
      val metrics = simpleFoldValidation(j, ens1, ens2, ens3)
      preList :::= List(metrics("precision"))
      precision += metrics("precision")
      recall += metrics("recall")
      accuracy += metrics("accuracy")
    }
    precision = precision/numFold
    recall = recall/numFold
    accuracy = accuracy/numFold
    val fmeasure = 2*(precision*recall)/(precision + recall)
    //calculate standard deviation
    var sum = 0.0
    preList.foreach{ p =>
      sum += Math.pow((p-precision), 2)
    }
    sum = sum/numFold
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
   * Calculates the precision, recall and accuracy of a fold
   * @param currentFold the number of current fold to validate
   * @param ens1 list of entities of first class
   * @param ens2 list of entities of second class
   * @param ens3 list of entities of third class
   * @return map with the corresponding values
   */
  private def simpleFoldValidation(currentFold: Int, ens1 : List[StringEntity], ens2 : List[StringEntity], ens3 : List[StringEntity]): Map[String, Double] = {
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
    val cls = new SimpleSimilarityClassifier(sc)
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
    val metrics = Map("precision" -> precision, "recall" -> recall, "accuracy" -> accuracy)
    metrics
  }

}
