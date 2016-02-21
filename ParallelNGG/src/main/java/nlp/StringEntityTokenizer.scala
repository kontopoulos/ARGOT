import org.apache.spark.rdd.RDD

/**
  * @author Kontopoulos Ioannis
  */
class StringEntityTokenizer extends EntityTokenizer with Serializable {

  /**
    * Returns an RDD with the words of the entity
    * @param e entity to be tokenized
    * @return rdd of words
    */
  override def getTokens(e: Entity): RDD[Atom] = {
    val en = e.asInstanceOf[StringEntity]
    val tokens: RDD[Atom] = en.dataStringRDD
      //split each line by spaces
      .flatMap(line => line.split(" "))
      //remove special characters from words
      .map(word => word.replaceAll("[,.?;:<>-]", ""))
      //remove empty strings that might occur from spaces
      .filter(word => word != "")
      //convert to string atoms
      .map(word => new StringAtom(word.hashCode, word))
    tokens
  }

  /**
    * Returns an rdd of atoms containing the character ngrams
    * The resulting ngrams might not be 100% accurate since we
    * will miss few ngrams from the beginning and the end of each
    * partition. Given that each partition can be several million
    * characters long, the loss in assurance should be negligible.
    * The main benefit here is that each partition can be executed in parallel
    * @param e entity to be tokenized
    * @param ngram size of ngram
    * @return array of string atoms
    */
  def getCharacterNGrams(e: Entity, ngram: Int): RDD[Atom] = {
    val en = e.asInstanceOf[StringEntity]
    en.dataStringRDD.mapPartitions(_.toList.mkString(" ").sliding(ngram))
      .map(atom => new StringAtom(("_" + atom).hashCode, "_" + atom))
  }

  /**
    * Returns an rdd of atoms containing the word ngrams
    * The resulting ngrams might not be 100% accurate since we
    * will miss few ngrams from the beginning and the end of each
    * partition. Given that each partition can be several million
    * characters long, the loss in assurance should be negligible.
    * The main benefit here is that each partition can be executed in parallel
    * @param e entity to be tokenized
    * @param ngram size of ngram
    * @return array of atoms
    */
  def getWordNGrams(e: Entity, ngram: Int): RDD[Atom] = {
    getTokens(e).map(_.dataStream)
      .mapPartitions(_.sliding(ngram))
      .map(a => new StringAtom(a.mkString(" ").hashCode, a.mkString(" ")))
  }

  /**
    * Returns an rdd of atoms containing ngrams of capital words and numbers
    * The resulting ngrams might not be 100% accurate since we
    * will miss few ngrams from the beginning and the end of each
    * partition. Given that each partition can be several million
    * characters long, the loss in assurance should be negligible.
    * The main benefit here is that each partition can be executed in parallel
    * @param e entity to be tokenized
    * @param ngram size of ngram
    * @return array of atoms
    */
  def getCapWordNGrams(e: Entity, ngram: Int): RDD[Atom] = {
    getCapitalizedAndNumbers(e).map(_.dataStream)
      .mapPartitions(_.sliding(ngram))
      .map(a => new StringAtom(a.mkString(" ").hashCode, a.mkString(" ")))
  }

  /**
    * Returns an RDD with the capitalized words
    * @param e entity to be tokenized
    * @return rdd of capitalized words
    */
  def getCapitalizedWords(e: Entity): RDD[Atom] = {
    val tokens = getCapitalizedAndNumbers(e)
      //filter to remove numeric values
      .filter(atom => !isNumeric(atom.dataStream))
    tokens
  }

  /**
    * Returns an RDD with the capitalized words
    * and the numbers of the entity
    * @param e entity to be tokenized
    * @return rdd of capitalized words and numbers
    */
  def getCapitalizedAndNumbers(e: Entity): RDD[Atom] = {
    val tokens = getTokens(e)
      //filter to take only Uppercase and numeric values
      .filter(atom => Character.isUpperCase(atom.dataStream.codePointAt(0)) || isNumeric(atom.dataStream))
    tokens
  }

  /**
    * Checks if a string is numeric
    * @param input string to check
    * @return true if numeric else false
    */
  private def isNumeric(input: String): Boolean = input.forall(_.isDigit)

}
