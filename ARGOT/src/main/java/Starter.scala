import experiments.CrossValidation
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * @author Kontopoulos Ioannis
 */
object Starter {
  def main(args: Array[String]) {
    // Apache spaRk based text mininG tOolkiT
    val conf = new SparkConf().setAppName("ARGOT")
      //.setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val numPartitions = args.head.toInt

    try {
      // spark context, directory with classes, number of folds
      val exp = new CrossValidation(sc,"docs",10)
      // current fold, number of partitions to use
      exp.classifyOnFold(0, numPartitions)
    }
    catch {
      case e: Exception =>
        println("ERROR! Check errors.log file.")
        val w = new java.io.FileWriter("errors.log",true)
        w.write(s"${e.getMessage}\n${e.getStackTrace.mkString("\n")}\n")
        w.close
    }
  }

}
