package gr.demokritos.iit.nGramGraphMethods

/**
 * @author Kontopoulos Ioannis
 */
class StringFixedNGramSegmentor(ngram: Int) extends Segmentor {
  
  /**
   * Segments a StringEntity into StringAtoms
   * @param e StringEntity
   * @return list of string atoms
   */
  override def getComponents(e: StringEntity): List[StringAtom] = {
    var atoms: List[StringAtom] = List()
    //begin index of string
    var begin = 0
    //create substrings based on n-gram size
    for (i <- 1 to e.dataString.length() - ngram + 1) {
        var a = new StringAtom()
        //end index of string
        var end = begin + ngram
        a.label = "_" + e.dataString.substring(begin, end)
        begin += 1
        atoms ::: List(a)
    }
    atoms
  }
  
}