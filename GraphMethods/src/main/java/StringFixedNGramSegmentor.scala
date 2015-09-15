/**
 * @author Kontopoulos Ioannis
 */
class StringFixedNGramSegmentor(val ngram: Int) extends Segmentor {

  /**
   * Segments a StringEntity into StringAtoms
   * @param e StringEntity
   * @return list of string atoms
   */
  override def getComponents(e: Entity): List[Atom] = {
    val en = e.asInstanceOf[StringEntity]
    var atoms: List[StringAtom] = Nil
    //begin index of string
    var begin = 0
    //create substrings based on ngram size
    for (i <- 1 to en.dataString.length() - ngram + 1) {
      //end index of string
      val end = begin + ngram
      val str = en.dataString.substring(begin, end)
      atoms :::= List(new StringAtom(str.hashCode.toString, "_" + str))
      begin += 1
    }
    atoms.reverse
  }

}
