package nlp

import org.apache.spark.rdd.RDD
import structs.DocumentAtom
import traits.{Atom, EntityTokenizer}

/**
  * @author Kontopoulos Ioannis
  */
class DocumentEntityTokenizer extends EntityTokenizer with Serializable {

  /**
    * Returns an RDD with the words of the entity
    * @param partitionedDocument document to be tokenized
    * @return rdd of atoms
    */
  override def getTokens(partitionedDocument: RDD[String]): RDD[Atom] = {
    val tokens: RDD[Atom] = partitionedDocument
      //split each line by spaces
      .flatMap(line => line.split("\\s+"))
      //remove special characters from words
      .map(word => word.replaceAll("[,.?;:<>-]", ""))
      //remove empty strings that might occur from spaces
      .filter(word => word != "")
      //convert to document atoms
      .map(word => DocumentAtom(word.hashCode, word))
    tokens
  }

  /**
    * Returns an rdd of atoms containing the character ngrams
    * The resulting ngrams might not be 100% accurate since we
    * will miss few ngrams from the beginning and the end of each
    * partition. Given that each partition can be several million
    * characters long, the loss in assurance should be negligible.
    * The main benefit here is that each partition can be executed in parallel
    * @param partitionedDocument document to be tokenized
    * @param ngram size of ngram
    * @return rdd of atoms
    */
  def getCharacterNGrams(partitionedDocument: RDD[String], ngram: Int): RDD[Atom] = {
    partitionedDocument.mapPartitions(_.toList.mkString(" ").sliding(ngram))
      .map(atom => DocumentAtom(atom.hashCode, atom))
  }

  /**
    * Returns an rdd of atoms containing the word ngrams
    * The resulting ngrams might not be 100% accurate since we
    * will miss few ngrams from the beginning and the end of each
    * partition. Given that each partition can be several million
    * characters long, the loss in assurance should be negligible.
    * The main benefit here is that each partition can be executed in parallel
    * @param partitionedDocument document to be tokenized
    * @param ngram size of ngram
    * @return rdd of atoms
    */
  def getWordNGrams(partitionedDocument: RDD[String], ngram: Int): RDD[Atom] = {
    getTokens(partitionedDocument).map(_.dataStream)
      .mapPartitions(_.sliding(ngram))
      .map(a => DocumentAtom(a.mkString(" ").hashCode, a.mkString(" ")))
  }

  /**
    * Returns an rdd of atoms containing ngrams of capital words and numbers
    * The resulting ngrams might not be 100% accurate since we
    * will miss few ngrams from the beginning and the end of each
    * partition. Given that each partition can be several million
    * characters long, the loss in assurance should be negligible.
    * The main benefit here is that each partition can be executed in parallel
    * @param partitionedDocument document to be tokenized
    * @param ngram size of ngram
    * @return rdd of atoms
    */
  def getCapWordNGrams(partitionedDocument: RDD[String], ngram: Int): RDD[Atom] = {
    getCapitalizedAndNumbers(partitionedDocument).map(_.dataStream)
      .mapPartitions(_.sliding(ngram))
      .map(a => DocumentAtom(a.mkString(" ").hashCode, a.mkString(" ")))
  }

  /**
    * Returns an RDD with the capitalized words
    * @param partitionedDocument document to be tokenized
    * @return rdd of capitalized words
    */
  def getCapitalizedWords(partitionedDocument: RDD[String]): RDD[Atom] = {
    val tokens = getCapitalizedAndNumbers(partitionedDocument)
      //filter to remove numeric values
      .filter(atom => !isNumeric(atom.dataStream))
    tokens
  }

  /**
    * Returns an RDD with the capitalized words
    * and the numbers of the entity
    * @param partitionedDocument document to be tokenized
    * @return rdd of capitalized words and numbers
    */
  def getCapitalizedAndNumbers(partitionedDocument: RDD[String]): RDD[Atom] = {
    val tokens = getTokens(partitionedDocument)
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
