/**
 * @author Kontopoulos Ioannis
 */
class StringFixedNGramSegmentingFunction(val ngram: Int) extends SegmentingFunction {

  /**
   * Segments a StringEntity into StringAtoms
   * @param e StringEntity
   * @return list of string atoms
   */
  override def getComponents(e: Entity): Array[Atom] = {
    val en = e.asInstanceOf[StringEntity]
    //slide by ngram step
    val atoms: Array[Atom] =  en.getPayload.sliding(ngram)
      //create indexed substrings
      .zipWithIndex
      //create String Atoms
      .map{case (s, i) => new StringAtom(s.hashCode.toString, "_" + s)}
      .toArray
    atoms
  }

}
