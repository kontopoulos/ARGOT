/**
 * @author Kontopoulos Ioannis
 */
class StringFixedNGramSegmentingFunction(val ngram: Int) extends SegmentingFunction {

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
      //increase frequency of occurrence of string atom
      atoms.filter( a => a.label == str.hashCode.toString).map( a => a.frequency += 1)
      begin += 1
    }
    atoms.reverse
  }

}
