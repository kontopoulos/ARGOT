/**
 * @author Kontopoulos Ioannis
 */
class StringEntity extends Entity {

  //actual string of the entity
  var dataString = ""
  //list of components of StringEntity
  private var components: List[StringAtom] = Nil

  /**
   * @return components of String Entity
   */
  override def getEntityComponents: List[StringAtom] = components

  /**
   * Returns the actual data of the entity
   * @return dataString
   */
  override def getPayload: String = {
    this.dataString
  }

  /**
   * Sets the list of components of the Entity
   * @param list list of components
   */
  def setEntityComponents(list: List[StringAtom]) = components = list

  /**
   * Reads dataString from a file
   * @param file file to be read
   */
  def readDataStringFromFile(file: String) = {
    try {
      val source = scala.io.Source.fromFile(file)
      dataString = source.mkString.replaceAll("\n", " ")
      source.close()
    }
    catch {
      case ex: Exception => println(ex.getMessage)
    }
  }

  /**
   * Finds the string atom by the label and retrieves
   * the frequency of string atom only after a graph
   * has been created from the entity
   * @param label label of string atom
   * @return frequency of occurrence in the document
   */
  def getFrequencyOfStringAtomByLabel(label: String): Int = {
    components.filter( a => a.label == label)(0).frequency
  }

}
