package gr.demokritos.iit.nGramGraphMethods

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
    var en:StringEntity = e.asInstanceOf[StringEntity]
    var atoms: List[StringAtom] = List()
    //begin index of string
    var begin = 0
    //create substrings based on n-gram size
    for (i <- 1 to en.dataString.length() - ngram + 1) {
        //end index of string
        var end = begin + ngram
        val str = "_" + en.dataString.substring(begin, end)
        atoms ::: List(new StringAtom(str))
        begin += 1
    }
    atoms
  }
  
}