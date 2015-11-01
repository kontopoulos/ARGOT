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
      .map{s => new StringAtom(s.hashCode.toString, "_" + s)}
      .toArray
    atoms
  }

}
