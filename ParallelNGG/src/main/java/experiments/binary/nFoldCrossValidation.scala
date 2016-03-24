import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
 * @author Kontopoulos Ioannis
 */
class nFoldCrossValidation(val sc: SparkContext, val numPartitions: Int, val numFold: Int) extends Experiment {

  /**
   * Runs the n-fold cross validation
   * using the Classifier selected
   * @param classifier classifier selection
   */
  def run(classifier: String) = {
    println("Classifier Selected: " + classifier)
    if (classifier == "SVM") runSVM
    else if (classifier == "NaiveBayes") runNaiveBayes
    else if (classifier == "simple") runSimple
    else println("Wrong Option!")
  }

  /**
   * Runs the n-fold cross validation using
   * Support Vector Machines
   * with Stochastic Gradient Descent Classifier
   */
  private def runSVM() = {
    println("Reading files...")
    val files1 = new java.io.File("C01/").listFiles.map( f => f.getAbsolutePath).toList.toArray
    val files2 = new java.io.File("C02/").listFiles.map( f => f.getAbsolutePath).toList.toArray
    println("Reading complete.")
    var fs = Array.empty[Double]
    var f = 0.0
    for (j <- 0 to numFold-1) {
      val metrics = svmFoldValidation(j, files1, files2)
      f += metrics
      fs :+= metrics
    }
    //calculate averaged metrics
    f = f/numFold
    //calculate standard deviation
    var sum = 0.0
    fs.foreach{ p =>
      sum += Math.pow((p-f), 2)
    }
    sum = sum/numFold
    val stdev = Math.sqrt(sum)
    //calculate standard error
    val sterr = stdev/(Math.sqrt(numFold))
    println("===================================")
    println("F-Measure = " + f)
    println("Standard Deviation = " + stdev)
    println("Standard Error = " + sterr)
    println("===================================")
  }

  /**
   * Calculates precision, recall, accuracy and f-measure
   * of current fold using Support Vector Machines
   * with Stochastic Gradient Descent Classifier
   * @param currentFold fold to validate
   * @param files1 array of files of first category
   * @param files2 array of files of second category
   * @return map with evaluation metrics
   */
  private def svmFoldValidation(currentFold: Int, files1: Array[String], files2: Array[String]): Double = {
    println("Separating training and testing datasets...")
    //get training and testing datasets from first category
    val testing1 = files1.slice(currentFold, currentFold+files1.length/numFold)
    val training1 = files1.slice(0, currentFold) ++ files1.slice(currentFold+files1.length/numFold, files1.length)
    //get training and testing datasets from second category
    val testing2 = files2.slice(currentFold, currentFold+files2.length/numFold)
    val training2 = files2.slice(0, currentFold) ++ files2.slice(currentFold+files2.length/numFold, files2.length)
    println("Separation complete.")
    println("Creating merging lineage...")
    val nggc = new NGramGraphCreator(3, 3)
    val m = new MergeOperator(0.5)
    //merge graphs from first training set to a class graph
    val e1 = new StringEntity
    e1.readFile(sc, training1.head, numPartitions)
    val g1 = nggc.getGraph(e1)
    val e2 = new StringEntity
    e2.readFile(sc, training1(1), numPartitions)
    val g2 = nggc.getGraph(e2)
    var classGraph1 = m.getResult(g1, g2)
    for (i <- 2 to training1.length-1) {
      val e = new StringEntity
      e.readFile(sc, training1(i), numPartitions)
      val g = nggc.getGraph(e)
      if (i % 30 == 0) {
        //materialize and store for future use
        classGraph1.edges.distinct.cache
        classGraph1.edges.distinct.count
        //every 30 iterations cut the lineage, due to long iteration
        classGraph1 = Graph(classGraph1.vertices.distinct, classGraph1.edges.distinct)
      }
      classGraph1 = m.getResult(classGraph1, g)
    }
    //merge graphs from second training set to a class graph
    val e3 = new StringEntity
    e3.readFile(sc, training2.head, numPartitions)
    val g3 = nggc.getGraph(e3)
    val e4 = new StringEntity
    e4.readFile(sc, training2(1), numPartitions)
    val g4 = nggc.getGraph(e4)
    var classGraph2 = m.getResult(g3, g4)
    for (i <- 2 to training2.size-1) {
      val e = new StringEntity
      e.readFile(sc, training2(i), numPartitions)
      val g = nggc.getGraph(e)
      if (i % 30 == 0) {
        //materialize and store for future use
        classGraph2.edges.distinct.cache
        classGraph2.edges.distinct.count
        //every 30 iterations cut the lineage, due to long iteration
        classGraph2 = Graph(classGraph2.vertices.distinct, classGraph2.edges.distinct)
      }
      classGraph2 = m.getResult(classGraph2, g)
    }
    //cache edges for future use
    classGraph1.edges.distinct.cache
    classGraph2.edges.distinct.cache
    println("Lineage complete.")
    //start training
    println("Creating feature vectors...")
    val cls = new SVMExperiment(sc, numPartitions)
    val model = cls.train(Array(classGraph1, classGraph2), training1, training2)
    //start testing
    println("Testing...")
    val metrics = cls.test(model, Array(classGraph1, classGraph2), testing1, testing2)
    //free memory of stored edges
    classGraph1.edges.distinct.unpersist()
    classGraph2.edges.distinct.unpersist()
    println("Testing complete")
    println("===========================")
    println("Fold Completed = " + (currentFold+1))
    println("===========================")
    metrics
  }

  /**
   * Run the n-fold cross validation
   * using Naive Bayes Classifier
   */
  private def runNaiveBayes() = {
    println("Reading files...")
    val files1 = new java.io.File("C01/").listFiles.map( f => f.getAbsolutePath).toList.toArray
    val files2 = new java.io.File("C02/").listFiles.map( f => f.getAbsolutePath).toList.toArray
    println("Reading complete.")
    var fs = Array.empty[Double]
    var f = 0.0
    for (j <- 0 to numFold-1) {
      val metrics = naiveBayesFoldValidation(j, files1, files2)
      f += metrics
      fs :+= metrics
    }
    //calculate averaged metrics
    f = f/numFold
    //calculate standard deviation
    var sum = 0.0
    fs.foreach{ p =>
      sum += Math.pow((p-f), 2)
    }
    sum = sum/numFold
    val stdev = Math.sqrt(sum)
    //calculate standard error
    val sterr = stdev/(Math.sqrt(numFold))
    println("===================================")
    println("F-Measure = " + f)
    println("Standard Deviation = " + stdev)
    println("Standard Error = " + sterr)
    println("===================================")
  }

  /**
   * Calculates precision, recall, accuracy and f-measure
   * of current fold using Naive Bayes Classifier
   * @param currentFold fold to validate
   * @param files1 array of files of first category
   * @param files2 array of files of second category
   * @return map with evaluation metrics
   */
  private def naiveBayesFoldValidation(currentFold: Int, files1: Array[String], files2: Array[String]): Double = {
    println("Separating training and testing datasets...")
    //get training and testing datasets from first category
    val testing1 = files1.slice(currentFold, currentFold+files1.length/numFold)
    val training1 = files1.slice(0, currentFold) ++ files1.slice(currentFold+files1.length/numFold, files1.length)
    //get training and testing datasets from second category
    val testing2 = files2.slice(currentFold, currentFold+files2.length/numFold)
    val training2 = files2.slice(0, currentFold) ++ files2.slice(currentFold+files2.size/numFold, files2.length)
    println("Separation complete.")
    println("Creating merging lineage...")
    val nggc = new NGramGraphCreator(3, 3)
    val m = new MergeOperator(0.5)
    //merge graphs from first training set to a class graph
    val e1 = new StringEntity
    e1.readFile(sc, training1.head, numPartitions)
    val g1 = nggc.getGraph(e1)
    val e2 = new StringEntity
    e2.readFile(sc, training1(1), numPartitions)
    val g2 = nggc.getGraph(e2)
    var classGraph1 = m.getResult(g1, g2)
    for (i <- 2 to training1.length-1) {
      val e = new StringEntity
      e.readFile(sc, training1(i), numPartitions)
      val g = nggc.getGraph(e)
      if (i % 30 == 0) {
        //materialize and store for future use
        classGraph1.edges.distinct.cache
        classGraph1.edges.distinct.count
        //every 30 iterations cut the lineage, due to long iteration
        classGraph1 = Graph(classGraph1.vertices.distinct, classGraph1.edges.distinct)
      }
      classGraph1 = m.getResult(classGraph1, g)
    }
    //merge graphs from second training set to a class graph
    val e3 = new StringEntity
    e3.readFile(sc, training2.head, numPartitions)
    val g3 = nggc.getGraph(e3)
    val e4 = new StringEntity
    e4.readFile(sc, training2(1), numPartitions)
    val g4 = nggc.getGraph(e4)
    var classGraph2 = m.getResult(g3, g4)
    for (i <- 2 to training2.length-1) {
      val e = new StringEntity
      e.readFile(sc, training2(i), numPartitions)
      val g = nggc.getGraph(e)
      if (i % 30 == 0) {
        //materialize and store for future use
        classGraph2.edges.distinct.cache
        classGraph2.edges.distinct.count
        //every 30 iterations cut the lineage, due to long iteration
        classGraph2 = Graph(classGraph2.vertices.distinct, classGraph2.edges.distinct)
      }
      classGraph2 = m.getResult(classGraph2, g)
    }
    //store edges for future use
    classGraph1.edges.distinct.cache
    classGraph2.edges.distinct.cache
    println("Lineage complete.")
    //start training
    println("Creating feature vectors...")
    val cls = new NaiveBayesExperiment(sc, numPartitions)
    val model = cls.train(Array(classGraph1, classGraph2), training1, training2)
    //start testing
    println("Testing...")
    val metrics = cls.test(model, Array(classGraph1, classGraph2), testing1, testing2)
    //free memory of stored edges
    classGraph1.edges.distinct.unpersist()
    classGraph2.edges.distinct.unpersist()
    println("Testing complete")
    println("===========================")
    println("Fold Completed = " + (currentFold+1))
    println("===========================")
    metrics
  }

  /**
   * Run the n-fold cross validation
   * using the simple classifier
   */
  private def runSimple() = {
    println("Reading files...")
    val files1 = new java.io.File("C01/").listFiles.map( f => f.getAbsolutePath).toList.toArray
    val files2 = new java.io.File("C02/").listFiles.map( f => f.getAbsolutePath).toList.toArray
    println("Reading complete.")
    var precision = 0.0
    var recall = 0.0
    var accuracy = 0.0
    var fmeasure = 0.0
    var preList = Array.empty[Double]
    for (j <- 0 to numFold-1) {
      val metrics = simpleFoldValidation(0, files1, files2)
      preList :+= metrics("precision")
      precision += metrics("precision")
      recall += metrics("recall")
      accuracy += metrics("accuracy")
      fmeasure += metrics("fmeasure")
    }
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
    println("Recall = " + recall)
    println("Accuracy = " + accuracy)
    println("F-measure = " + fmeasure)
    println("Standard Deviation of Precision = " + stdev)
    println("Standard Error of Precision = " + sterr)
    println("===================================")
  }

  /**
   * Calculates precision, recall and accuracy
   * of current fold using the simple classifier
   * @param currentFold fold to validate
   * @param files1 array of files of first category
   * @param files2 array of files of second category
   * @return map with evaluation metrics
   */
  private def simpleFoldValidation(currentFold: Int, files1: Array[String], files2: Array[String]): Map[String, Double] = {
    println("Separating training and testing datasets...")
    //get training and testing datasets from first category
    val testing1 = files1.slice(currentFold, currentFold+files1.length/numFold)
    val training1 = files1.slice(0, currentFold) ++ files1.slice(currentFold+files1.length/numFold, files1.length)
    //get training and testing datasets from second category
    val testing2 = files2.slice(currentFold, currentFold+files2.length/numFold)
    val training2 = files2.slice(0, currentFold) ++ files2.slice(currentFold+files2.length/numFold, files2.length)
    println("Separation complete.")
    println("Creating merging lineage...")
    //start training upon datasets
    val cls = new SimilarityExperiment(sc, numPartitions)
    val g01 = cls.train(training1)
    val g02 = cls.train(training2)
    println("Lineage complete.")
    //store edges for future use
    g01.edges.distinct.cache
    g02.edges.distinct.cache
    println("Testing...")
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
    testing1.foreach{ f =>
      val label = cls.test(f, Array(g01, g02))
      if (label.head == "C01") {
        //true positive for C01
        tp01 += 1
        //true negative for C02
        tn02 += 1
      }
      else if (label.head == "C02") {
        //false positive for C02
        fp02 += 1
        //false negative for C01
        fn01 += 1
      }
    }
    testing2.foreach{ f =>
      val label = cls.test(f, Array(g01, g02))
      if (label.head == "C02") {
        //true positive for C02
        tp02 += 1
        //true negative for C01
        tn01 += 1
      }
      else if (label.head == "C01") {
        //false positive for C01
        fp01 += 1
        //false negative for C02
        fn02 += 1
      }
    }
    //free memory of stored edges
    g01.edges.distinct.unpersist()
    g02.edges.distinct.unpersist()
    //calculate precision
    var precision1 = 0.0
    if ((tp01 + fp01) != 0) {
      precision1 = tp01.toDouble/(tp01 + fp01)
    }
    var precision2 = 0.0
    if ((tp02 + fp02) != 0) {
      precision2 = tp02.toDouble/(tp02 + fp02)
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
    //calculate accuracy
    var accuracy1 = 0.0
    if ((tp01 + fp01 + tn01 + fn01) != 0) {
      accuracy1 = (tp01 + tn01).toDouble/(tp01 + fp01 + tn01 + fn01)
    }
    var accuracy2 = 0.0
    if ((tp02 + fp02 + tn02 + fn02) != 0) {
      accuracy2 = (tp02 + tn02).toDouble/(tp02 + fp02 + tn02 + fn02)
    }
    //calculate averaged values
    val precision = (precision1 + precision2)/2
    val recall = (recall1 + recall2)/2
    val accuracy = (accuracy1 + accuracy2)/2
    val fmeasure = 2*(precision*recall)/(precision + recall)
    println("Testing complete.")
    println("===================================")
    println("Fold Completed = " + (currentFold + 1))
    println("===================================")
    val metrics = Map("precision" -> precision, "recall" -> recall, "accuracy" -> accuracy, "fmeasure" -> fmeasure)
    metrics
  }

}
